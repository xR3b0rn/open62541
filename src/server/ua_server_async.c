/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2019 (c) Fraunhofer IOSB (Author: Klaus Schick)
 *    Copyright 2019, 2025 (c) Fraunhofer IOSB (Author: Julius Pfrommer)
 *    Copyright 2026 (c) o6 Automation GmbH (Author: Julius Pfrommer)
 */

#include "ua_server_async.h"
#include "ua_server_internal.h"

/* The layout of the results array is:
 * [results-array] | padding | UA_AsyncResponse | padding |
 * [UA_AsyncOperation-array] | padding | [worker-buffer-array]
 *
 * The worker-buffer-array reserves one resultsType-sized slot per
 * operation. For READ/CALL requests, this is the dedicated buffer handed
 * to a still-pending operation's worker (see UA_AsyncOperation::output /
 * responseSlot) -- kept separate from the results-array itself so a
 * worker can safely own its slot until UA_Server_setAsync*Result(), with
 * no risk of racing directOpCallback()/sendAsyncResponse() reading the
 * response concurrently. Reserved but unused for WRITE, where there is no
 * such race (UA_Server_setAsyncWriteResult() always writes the result
 * itself, under the lock) -- kept uniform here for simplicity.
 *
 * We need to take care about memory alignment (padding). */
static void *
allocateResultsArray(const UA_DataType *resultsType, size_t resultsLen,
                     UA_AsyncResponse **resp, UA_AsyncOperation **ops,
                     void **workerBufs) {
    const size_t padding = sizeof(size_t) - 1;
    const size_t fixedSize = sizeof(UA_AsyncResponse) + 3 * padding;
    const size_t elementSize =
        2 * resultsType->memSize + sizeof(UA_AsyncOperation);

    /* Reserve the maximum padding at both alignment boundaries. */
    if(resultsLen > (SIZE_MAX - fixedSize) / elementSize)
        return NULL;

    size_t responseBegin =
        (resultsType->memSize * resultsLen + padding) & ~padding;
    size_t opsBegin =
        (responseBegin + sizeof(UA_AsyncResponse) + padding) & ~padding;
    size_t workerBufsBegin =
        (opsBegin + sizeof(UA_AsyncOperation) * resultsLen + padding) & ~padding;
    size_t allocationSize =
        workerBufsBegin + resultsType->memSize * resultsLen;

    void *arr = UA_calloc(1, allocationSize);
    if(!arr)
        return NULL;
    uintptr_t arrMem = (uintptr_t)arr;
    *resp = (UA_AsyncResponse*)(arrMem + responseBegin);
    *ops = (UA_AsyncOperation*)(arrMem + opsBegin);
    *workerBufs = (void*)(arrMem + workerBufsBegin);
    return arr;
}

/* Cancel the operation, but don't _clear it here */
static void
UA_AsyncOperation_cancel(UA_Server *server, UA_AsyncOperation *op,
                         UA_StatusCode opstatus) {
    UA_ServerConfig *sc = &server->config;
    void *cancelPtr = NULL;

    switch(op->asyncOperationType) {
    case UA_ASYNCOPERATIONTYPE_READ_REQUEST:
        cancelPtr = op->output.read;
        op->responseSlot.read->hasStatus = true;
        op->responseSlot.read->status = opstatus;
        break;
    case UA_ASYNCOPERATIONTYPE_READ_DIRECT:
        cancelPtr = &op->output.directRead;
        op->cancelStatus = opstatus;
        break;
    case UA_ASYNCOPERATIONTYPE_WRITE_REQUEST:
        cancelPtr = &op->context.writeValue.value;
        *op->output.write = opstatus;
        break;
    case UA_ASYNCOPERATIONTYPE_WRITE_DIRECT:
        cancelPtr = &op->context.writeValue.value;
        op->output.directWrite = opstatus;
        break;
    case UA_ASYNCOPERATIONTYPE_CALL_REQUEST:
        cancelPtr = op->output.call->outputArguments;
        op->responseSlot.call->statusCode = opstatus;
        break;
    case UA_ASYNCOPERATIONTYPE_CALL_DIRECT:
        cancelPtr = op->output.directCall.outputArguments;
        op->cancelStatus = opstatus;
        break;
    default: UA_assert(false); return;
    }

    /* Notify the application that it must no longer set the async result */
    if(sc->asyncOperationCancelCallback)
        sc->asyncOperationCancelCallback(server, cancelPtr);

    op->status = UA_ASYNCOPERATIONSTATUS_CANCELED_WAITING_FOR_WORKER;
}

static void
UA_AsyncOperation_delete(UA_AsyncOperation *op) {
    UA_assert(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT);
    switch(op->asyncOperationType) {
    case UA_ASYNCOPERATIONTYPE_READ_DIRECT:
        UA_DataValue_clear(&op->output.directRead);
        break;
    case UA_ASYNCOPERATIONTYPE_WRITE_DIRECT:
        break;
    case UA_ASYNCOPERATIONTYPE_CALL_DIRECT:
        UA_CallMethodResult_clear(&op->output.directCall);
        break;
    default: UA_assert(false); break;
    }
    UA_free(op);
}

static void
UA_AsyncResponse_delete(UA_AsyncResponse *ar) {
    UA_NodeId_clear(&ar->sessionId);

    /* Clean up the results array last. Because the results array memory also
     * includes ar. */
    void *arr = NULL;
    size_t arrSize = 0;
    const UA_DataType *arrType;
    if(ar->responseType == &UA_TYPES[UA_TYPES_CALLRESPONSE]) {
        arr = ar->response.callResponse.results;
        arrSize = ar->response.callResponse.resultsSize;
        ar->response.callResponse.results = NULL;
        ar->response.callResponse.resultsSize = 0;
        arrType = &UA_TYPES[UA_TYPES_CALLMETHODRESULT];
    } else if(ar->responseType == &UA_TYPES[UA_TYPES_READRESPONSE]) {
        arr = ar->response.readResponse.results;
        arrSize = ar->response.readResponse.resultsSize;
        ar->response.readResponse.results = NULL;
        ar->response.readResponse.resultsSize = 0;
        arrType = &UA_TYPES[UA_TYPES_DATAVALUE];
    } else /* if(ar->responseType == &UA_TYPES[UA_TYPES_WRITERESPONSE]) */ {
        UA_assert(ar->responseType == &UA_TYPES[UA_TYPES_WRITERESPONSE]);
        arr = ar->response.writeResponse.results;
        arrSize = ar->response.writeResponse.resultsSize;
        ar->response.writeResponse.results = NULL;
        ar->response.writeResponse.resultsSize = 0;
        arrType = &UA_TYPES[UA_TYPES_STATUSCODE];
    }
    UA_clear(&ar->response.callResponse, ar->responseType);
    UA_Array_delete(arr, arrSize, arrType);
}

static void
notifyServiceEnd(UA_Server *server, UA_AsyncResponse *ar,
                 UA_Session *session, UA_SecureChannel *sc) {
    /* Collect the payload */
    UA_NodeId sessionId = (session) ? session->sessionId : UA_NODEID_NULL;
    UA_UInt32 secureChannelId = (sc) ? sc->securityToken.channelId : 0;
    UA_NodeId serviceTypeId;
    if(ar->responseType == &UA_TYPES[UA_TYPES_CALLRESPONSE]) {
        serviceTypeId = UA_TYPES[UA_TYPES_CALLREQUEST].typeId;
    } else if(ar->responseType == &UA_TYPES[UA_TYPES_READRESPONSE]) {
        serviceTypeId = UA_TYPES[UA_TYPES_READREQUEST].typeId;
    } else /* if(ar->responseType == &UA_TYPES[UA_TYPES_WRITERESPONSE]) */ {
        serviceTypeId = UA_TYPES[UA_TYPES_WRITEREQUEST].typeId;
    }

    /* Notify the application */
    UA_STATIC_THREAD_LOCAL UA_KeyValuePair notifyPayload[4] = {
        {{0, UA_STRING_STATIC("securechannel-id")}, {0}},
        {{0, UA_STRING_STATIC("session-id")}, {0}},
        {{0, UA_STRING_STATIC("request-id")}, {0}},
        {{0, UA_STRING_STATIC("service-type")}, {0}}
    };
    UA_KeyValueMap notifyPayloadMap = {4, notifyPayload};
    UA_Variant_setScalar(&notifyPayload[0].value, &secureChannelId,
                         &UA_TYPES[UA_TYPES_UINT32]);
    UA_Variant_setScalar(&notifyPayload[1].value, &sessionId,
                         &UA_TYPES[UA_TYPES_NODEID]);
    UA_Variant_setScalar(&notifyPayload[2].value, &ar->uacpRequestId,
                         &UA_TYPES[UA_TYPES_UINT32]);
    UA_Variant_setScalar(&notifyPayload[3].value, &serviceTypeId,
                         &UA_TYPES[UA_TYPES_NODEID]);

    UA_ApplicationNotificationType nt = UA_APPLICATIONNOTIFICATIONTYPE_SERVICE_END;
    notifyApplication(server, nt, notifyPayloadMap);
}

static void
sendAsyncResponse(UA_Server *server, UA_AsyncResponse *ar) {
    UA_assert(ar->opCountdown == 0);

    if(ar->abandoned) {
        UA_LOG_DEBUG(server->config.logging, UA_LOGCATEGORY_SERVER,
                     "Async response for closed transport carrier token %"
                     PRIu64 " was abandoned", ar->responseToken);
        return;
    }

    /* Get the session */
    UA_Session *session = getSessionById(server, &ar->sessionId);
    UA_SecureChannel *channel = (session) ? session->channel : NULL;

    /* Notify that processing the service has ended */
    notifyServiceEnd(server, ar, session, channel);

    /* Check the session */
    if(!session) {
        UA_LOG_WARNING(server->config.logging, UA_LOGCATEGORY_SERVER,
                       "Async Service: Session %N no longer exists", ar->sessionId);
        return;
    }

    /* Check the channel */
    if(!channel) {
        UA_LOG_WARNING_SESSION(server->config.logging, session,
                               "Async Service Response cannot be sent. "
                               "No SecureChannel for the session.");
        return;
    }

    /* Set the request handle */
    UA_ResponseHeader *responseHeader = (UA_ResponseHeader*)
        &ar->response.callResponse.responseHeader;
    responseHeader->requestHandle = ar->requestHandle;

    /* Send the Response */
    UA_StatusCode res = sendResponse(server, channel, ar->responseToken,
                                     (UA_Response*)&ar->response, ar->responseType);
    if(res != UA_STATUSCODE_GOOD) {
        UA_LOG_WARNING_SESSION(server->config.logging, session,
                               "Async response for token %" PRIu64 " failed "
                               "with StatusCode %s", ar->responseToken,
                               UA_StatusCode_name(res));
    }
}

static void
directOpCallback(UA_Server *server, UA_AsyncOperation *op) {
    UA_Boolean canceled =
        (op->status == UA_ASYNCOPERATIONSTATUS_CANCELED_WAITING_FOR_WORKER);

    switch(op->asyncOperationType) {
    case UA_ASYNCOPERATIONTYPE_READ_DIRECT: {
        if(!canceled) {
            op->handling.callback.method.read(server, op->handling.callback.context,
                                              &op->output.directRead);
            break;
        }
        UA_DataValue dummy;
        UA_DataValue_init(&dummy);
        dummy.hasStatus = true;
        dummy.status = op->cancelStatus;
        op->handling.callback.method.read(server, op->handling.callback.context,
                                          &dummy);
        break;
    }
    case UA_ASYNCOPERATIONTYPE_WRITE_DIRECT:
        op->handling.callback.method.write(server, op->handling.callback.context,
                                           op->output.directWrite);
        break;
    case UA_ASYNCOPERATIONTYPE_CALL_DIRECT: {
        if(!canceled) {
            op->handling.callback.method.call(server, op->handling.callback.context,
                                              &op->output.directCall);
            break;
        }
        UA_CallMethodResult dummy;
        UA_CallMethodResult_init(&dummy);
        dummy.statusCode = op->cancelStatus;
        op->handling.callback.method.call(server, op->handling.callback.context,
                                          &dummy);
        break;
    }
    default: UA_assert(false); break;
    }
}

/* Called from the EventLoop via a delayed callback */
static void
UA_AsyncManager_processReady(void *application /* UA_Server */,
                             void *context /* UA_AsyncManager */) {
    UA_Server *server = (UA_Server*)application;
    UA_AsyncManager *am = (UA_AsyncManager*)context;
    lockServer(server);

    /* Reset the delayed callback */
    UA_atomic_store((UA_atomic(void*)*)&am->dc.callback, NULL);

    /* Process ready direct operations and free them */
    UA_AsyncOperation *op = NULL, *op_tmp = NULL;
    TAILQ_FOREACH_SAFE(op, &am->readyOps, pointers, op_tmp) {
        TAILQ_REMOVE(&am->readyOps, op, pointers);
        directOpCallback(server, op);
        if(op->status == UA_ASYNCOPERATIONSTATUS_FINISHED) {
            am->opsCount--;
            UA_AsyncOperation_delete(op);
        } else {
            TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        }
    }

    /* Send out ready responses */
    UA_AsyncResponse *ar, *temp;
    TAILQ_FOREACH_SAFE(ar, &am->readyResponses, pointers, temp) {
        TAILQ_REMOVE(&am->readyResponses, ar, pointers);
        sendAsyncResponse(server, ar);
        if(ar->zombieCount == 0) {
            UA_AsyncResponse_delete(ar);
        } else {
            TAILQ_INSERT_TAIL(&am->zombieResponses, ar, pointers);
        }
    }

    unlockServer(server);
}

static void
processReadyLater(UA_Server *server) {
    UA_AsyncManager *am = &server->asyncManager;
    /* UA_AsyncManager_clear drains ready work synchronously after stop. */
    if(am->dc.callback != NULL ||
       server->state == UA_LIFECYCLESTATE_STOPPED)
        return;

    UA_EventLoop *el = server->config.eventLoop;
    am->dc.callback = UA_AsyncManager_processReady;
    am->dc.application = server;
    am->dc.context = am;
    el->addDelayedCallback(el, &am->dc);
    el->cancel(el); /* Wake up the EventLoop if currently waiting in select() */
}

static void
processOperationResult(UA_Server *server, UA_AsyncOperation *op) {
    UA_AsyncManager *am = &server->asyncManager;
    if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
        /* Direct operation */
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&am->readyOps, op, pointers);
    } else {
        /* Part of a service request */
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        am->opsCount--;

        UA_AsyncResponse *ar = op->handling.response;

        /* This operation was force-completed and is still awaiting a late
         * worker acknowledgement. Its memory is shared with (part of) the
         * UA_AsyncResponse, so it cannot be freed on its own. Keep it
         * findable in zombieOps and mark the response so it is not freed
         * once sent until this operation was acknowledged. */
        if(op->status == UA_ASYNCOPERATIONSTATUS_CANCELED_WAITING_FOR_WORKER) {
            ar->zombieCount++;
            TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        }

        ar->opCountdown -= 1;
        if(ar->opCountdown > 0)
            return;

        /* Enqueue ar in the readyResponses */
        TAILQ_REMOVE(&am->waitingResponses, ar, pointers);
        TAILQ_INSERT_TAIL(&am->readyResponses, ar, pointers);
    }

    /* Trigger the main server thread to handle ready operations and responses */
    processReadyLater(server);
}

/* Force-complete an operation that has ALREADY been unlinked from
 * am->waitingOps by the caller. UA_AsyncOperation_cancel() invokes the
 * user's cancellation callback, which may reenter the server (e.g. a
 * worker racing the cancellation calling UA_Server_setAsync*Result from
 * inside the callback) -- unlinking first ensures a reentrant call cannot
 * find and process the same operation again, matching the discipline
 * cancelAsyncResponseOperations() and UA_AsyncManager_cancelSession()
 * already use. This then finishes the bookkeeping that
 * processOperationResult() does for a still-linked operation, specialized
 * for the fact that this operation is always force-completed (so, for a
 * request-backed operation, always ends up a zombie -- unlike
 * processOperationResult(), which also handles genuine, non-cancelled
 * completions). */
static void
finishCanceledOp(UA_Server *server, UA_AsyncOperation *op, UA_StatusCode status) {
    UA_AsyncManager *am = &server->asyncManager;

    UA_AsyncOperation_cancel(server, op, status);

    if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
        /* Direct operation. Delivered (and, if genuinely finished by then,
         * freed) by UA_AsyncManager_processReady(). */
        TAILQ_INSERT_TAIL(&am->readyOps, op, pointers);
        processReadyLater(server);
        return;
    }

    /* Part of a service request. Its memory is shared with (part of) the
     * UA_AsyncResponse, so it cannot be freed on its own -- keep it
     * findable in zombieOps until the worker's belated
     * UA_Server_setAsync*Result call. */
    UA_AsyncResponse *ar = op->handling.response;
    ar->zombieCount++;
    TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);

    am->opsCount--;
    ar->opCountdown -= 1;
    if(ar->opCountdown > 0)
        return;

    TAILQ_REMOVE(&am->waitingResponses, ar, pointers);
    TAILQ_INSERT_TAIL(&am->readyResponses, ar, pointers);
    processReadyLater(server);
}

/* Check if any operations have timed out */
static void
checkTimeouts(UA_Server *server, void *_) {
    /* Timeouts are not configured */
    if(server->config.asyncOperationTimeout <= 0.0)
        return;

    lockServer(server);

    UA_EventLoop *el = server->config.eventLoop;
    UA_AsyncManager *am = &server->asyncManager;
    const UA_DateTime tNow = el->dateTime_nowMonotonic(el);

    /* Unlink all timed-out operations first -- see finishCanceledOp(). */
    TAILQ_HEAD(, UA_AsyncOperation) timedOutOps;
    TAILQ_INIT(&timedOutOps);
    UA_AsyncOperation *op = NULL, *op_tmp = NULL;
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        /* Check the timeout */
        if(op->asyncOperationType <= UA_ASYNCOPERATIONTYPE_WRITE_REQUEST) {
            if(tNow <= op->handling.response->timeout)
                continue;
        } else {
            if(tNow <= op->handling.callback.timeout)
                continue;
        }
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&timedOutOps, op, pointers);
    }

    while((op = TAILQ_FIRST(&timedOutOps))) {
        TAILQ_REMOVE(&timedOutOps, op, pointers);
        /* TAILQ_REMOVE does not clear the links in release builds. Avoid
         * retaining the stack-local queue head across the callback. */
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;
        UA_LOG_WARNING(server->config.logging, UA_LOGCATEGORY_SERVER,
                       "Operation was removed due to a timeout");
        finishCanceledOp(server, op, UA_STATUSCODE_BADTIMEOUT);
    }

    unlockServer(server);
}

void
UA_AsyncManager_init(UA_AsyncManager *am, UA_Server *server) {
    memset(am, 0, sizeof(UA_AsyncManager));
    TAILQ_INIT(&am->waitingResponses);
    TAILQ_INIT(&am->readyResponses);
    TAILQ_INIT(&am->zombieResponses);
    TAILQ_INIT(&am->waitingOps);
    TAILQ_INIT(&am->readyOps);
    TAILQ_INIT(&am->zombieOps);
}

void UA_AsyncManager_start(UA_AsyncManager *am, UA_Server *server) {
    /* Add a regular callback for cleanup and sending finished responses at a
     * 1s interval. */
    UA_StatusCode res = addRepeatedCallback(server, (UA_ServerCallback)checkTimeouts,
                    NULL, 1000.0, &am->checkTimeoutCallbackId);
    if(res != UA_STATUSCODE_GOOD) {
        UA_LOG_WARNING(server->config.logging, UA_LOGCATEGORY_SERVER,
                    "Failed to register async timeout callback. "
                    "Async operations will not be cleaned up on timeout. StatusCode: %s",
                    UA_StatusCode_name(res));
        am->checkTimeoutCallbackId = 0;
    }
}

void UA_AsyncManager_stop(UA_AsyncManager *am, UA_Server *server) {
    removeCallback(server, am->checkTimeoutCallbackId);
    if(am->dc.callback) {
        UA_EventLoop *el = server->config.eventLoop;
        el->removeDelayedCallback(el, &am->dc);
    }
}

void
UA_AsyncManager_clear(UA_AsyncManager *am, UA_Server *server) {
    UA_LOCK_ASSERT(&server->serviceMutex);

    /* Cancel all operations. This moves all operations and responses into
     * the ready state. Unlink everything first -- see finishCanceledOp(). */
    TAILQ_HEAD(, UA_AsyncOperation) canceledOps;
    TAILQ_INIT(&canceledOps);
    UA_AsyncOperation *op, *op_tmp;
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&canceledOps, op, pointers);
    }
    while((op = TAILQ_FIRST(&canceledOps))) {
        TAILQ_REMOVE(&canceledOps, op, pointers);
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;
        finishCanceledOp(server, op, UA_STATUSCODE_BADSHUTDOWN);
    }

    /* This sends out/notifies and removes all direct operations and async requests */
    UA_AsyncManager_processReady(server, am);

    /* Force-free any zombies left over -- both those just created above and
     * any older ones still waiting for a worker that never acknowledged
     * them. The server is being torn down, so there is no "later" left to
     * wait for a late acknowledgement. A worker that still calls
     * UA_Server_setAsync*Result afterwards simply won't find the operation
     * any more and gets UA_STATUSCODE_BADNOTFOUND -- safe, as long as the
     * UA_Server itself is not also already freed by then. The application
     * is responsible for stopping/joining any worker threads that could
     * call UA_Server_setAsync*Result before UA_Server_delete completes. */
    TAILQ_FOREACH_SAFE(op, &am->zombieOps, pointers, op_tmp) {
        TAILQ_REMOVE(&am->zombieOps, op, pointers);
        /* Request-type operations share memory with their UA_AsyncResponse
         * and are freed together with it below -- but READ/CALL's
         * dedicated worker buffer (see UA_AsyncOperation::output) may
         * still hold nested data (e.g. a UA_Variant's array buffer) that
         * nothing else will free; see finalizeZombieOp(). */
        if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
            am->opsCount--;
            UA_AsyncOperation_delete(op);
        } else if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_READ_REQUEST) {
            UA_DataValue_clear(op->output.read);
        } else if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_REQUEST) {
            UA_CallMethodResult_clear(op->output.call);
        }
    }
    UA_AsyncResponse *ar, *ar_tmp;
    TAILQ_FOREACH_SAFE(ar, &am->zombieResponses, pointers, ar_tmp) {
        TAILQ_REMOVE(&am->zombieResponses, ar, pointers);
        UA_AsyncResponse_delete(ar);
    }

    UA_assert(am->opsCount == 0);
}

void
UA_AsyncManager_cancelSession(UA_Server *server, const UA_NodeId *sessionId,
                              UA_StatusCode status) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_AsyncManager *am = &server->asyncManager;
    TAILQ_HEAD(, UA_AsyncResponse) canceledResponses;
    TAILQ_HEAD(, UA_AsyncOperation) canceledOps;
    TAILQ_INIT(&canceledResponses);
    TAILQ_INIT(&canceledOps);

    /* Unlink all matching responses and operations before invoking user
     * cancellation callbacks, which may reenter and mutate the manager. */
    UA_AsyncResponse *ar, *ar_tmp;
    TAILQ_FOREACH_SAFE(ar, &am->waitingResponses, pointers, ar_tmp) {
        if(!UA_NodeId_equal(&ar->sessionId, sessionId))
            continue;
        TAILQ_REMOVE(&am->waitingResponses, ar, pointers);
        TAILQ_INSERT_TAIL(&canceledResponses, ar, pointers);
    }

    UA_AsyncOperation *op, *op_tmp;
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT ||
           !UA_NodeId_equal(&op->handling.response->sessionId, sessionId))
            continue;
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&canceledOps, op, pointers);
        am->opsCount--;
        UA_assert(op->handling.response->opCountdown > 0);
        op->handling.response->opCountdown--;
    }

    while((op = TAILQ_FIRST(&canceledOps))) {
        TAILQ_REMOVE(&canceledOps, op, pointers);
        /* TAILQ_REMOVE does not clear the links in release builds. Avoid
         * retaining the stack-local queue head across the callback. */
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;
        UA_AsyncOperation_cancel(server, op, status);

        /* The operation is now CANCELED_WAITING_FOR_WORKER -- park it as a
         * zombie exactly like finishCanceledOp()/processOperationResult()
         * do, since its memory is shared with (part of) its
         * UA_AsyncResponse and a worker that has not yet noticed the
         * cancellation may still be writing into it. Safe to do
         * unconditionally here (unlike at the point an operation first
         * goes async): ar is already fully persisted and, from here on,
         * reachable only through the async manager itself -- never
         * through a synchronous caller about to free it. */
        UA_AsyncResponse *canceledAr = op->handling.response;
        canceledAr->zombieCount++;
        TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
    }

    UA_Boolean responseReady = false;
    while((ar = TAILQ_FIRST(&canceledResponses))) {
        TAILQ_REMOVE(&canceledResponses, ar, pointers);
        UA_assert(ar->opCountdown == 0);
        TAILQ_INSERT_TAIL(&am->readyResponses, ar, pointers);
        responseReady = true;
    }
    if(responseReady)
        processReadyLater(server);
}

UA_UInt32
UA_AsyncManager_cancel(UA_Server *server, UA_Session *session, UA_UInt32 requestHandle) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_AsyncManager *am = &server->asyncManager;

    /* Unlink all matching operations first -- see finishCanceledOp(). */
    TAILQ_HEAD(, UA_AsyncOperation) canceledOps;
    TAILQ_INIT(&canceledOps);
    UA_UInt32 count = 0;
    UA_AsyncOperation *op, *op_tmp;
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        /* Only request operations own a handling.response. */
        if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT)
            continue;
        UA_AsyncResponse *ar = op->handling.response;
        if(ar->requestHandle != requestHandle ||
           !UA_NodeId_equal(&session->sessionId, &ar->sessionId))
            continue;

        count++; /* Found a matching request */

        /* Set the status of the overall response */
        ar->response.callResponse.responseHeader.serviceResult =
            UA_STATUSCODE_BADREQUESTCANCELLEDBYCLIENT;

        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&canceledOps, op, pointers);
    }

    while((op = TAILQ_FIRST(&canceledOps))) {
        TAILQ_REMOVE(&canceledOps, op, pointers);
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;
        finishCanceledOp(server, op, UA_STATUSCODE_BADOPERATIONABANDONED);
    }

    return count;
}

void
UA_AsyncManager_abandon(UA_Server *server, UA_SecureChannel *channel,
                        UA_UInt64 responseToken) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_AsyncManager *am = &server->asyncManager;
    UA_AsyncResponse *ar;
    TAILQ_FOREACH(ar, &am->waitingResponses, pointers) {
        if(ar->responseToken != responseToken)
            continue;
        UA_Session *session = getSessionById(server, &ar->sessionId);
        if(session && session->channel == channel)
            ar->abandoned = true;
    }
    TAILQ_FOREACH(ar, &am->readyResponses, pointers) {
        if(ar->responseToken != responseToken)
            continue;
        UA_Session *session = getSessionById(server, &ar->sessionId);
        if(session && session->channel == channel)
            ar->abandoned = true;
    }
}

static void
persistAsyncResponse(UA_Server *server, UA_Session *session,
                     void *response, UA_AsyncResponse *ar) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_AsyncManager *am = &server->asyncManager;

    /* Pending results, attach the AsyncResponse to the AsyncManager. The
     * transport correlation token, optional UACP RequestId and client-supplied
     * RequestHandle are set before processing the request. */
    ar->responseToken = am->currentResponseToken;
    ar->uacpRequestId = am->currentUacpRequestId;
    ar->requestHandle = am->currentRequestHandle;
    ar->sessionId = session->sessionId;
    ar->timeout = UA_INT64_MAX;

    UA_EventLoop *el = server->config.eventLoop;
    if(server->config.asyncOperationTimeout > 0.0)
        ar->timeout = el->dateTime_nowMonotonic(el) + (UA_DateTime)
            (server->config.asyncOperationTimeout * (UA_DateTime)UA_DATETIME_MSEC);

    /* Move the response content to the AsyncResponse */
    memcpy(&ar->response, response, ar->responseType->memSize);
    UA_init(response, ar->responseType);

    if(ar->opCountdown > 0) {
        /* Enqueue the ar, waiting for its remaining operations. */
        TAILQ_INSERT_TAIL(&am->waitingResponses, ar, pointers);
        return;
    }

    /* Every operation already "completed" from opCountdown's point of view
     * -- persistAsyncResponseOperation() force-completed one at the queue
     * limit without ever counting it there, and it was the only/last
     * operation of this request. ar->zombieCount > 0 (the caller only
     * persists here when opCountdown or zombieCount is nonzero) means it
     * is not actually safe to hand the response back to the RPC caller and
     * free it synchronously: a worker may still be writing into that
     * force-completed operation's output memory. Go straight to
     * readyResponses instead of waitingResponses, which would never be
     * drained again since nothing will further decrement opCountdown. */
    UA_assert(ar->zombieCount > 0);
    TAILQ_INSERT_TAIL(&am->readyResponses, ar, pointers);
    processReadyLater(server);
}

static void
persistAsyncResponseOperation(UA_Server *server, UA_AsyncOperation *op,
                              UA_AsyncOperationType opType, UA_AsyncResponse *ar) {
    /* op->output (and, for READ_REQUEST/CALL_REQUEST, op->responseSlot)
     * must already be set up by the caller. */
    op->asyncOperationType = opType;
    op->handling.response = ar;

    /* Not enough resources to store the async operation */
    UA_AsyncManager *am = &server->asyncManager;
    if(server->config.maxAsyncOperationQueueSize != 0 &&
       am->opsCount >= server->config.maxAsyncOperationQueueSize) {
        UA_LOG_WARNING(server->config.logging, UA_LOGCATEGORY_SERVER,
                       "Cannot create async operation: Queue exceeds limit (%d).",
                       (int unsigned)server->config.maxAsyncOperationQueueSize);
        /* The operation's callback (Operation_Read() et al., called before
         * this) has already run and may have handed the output pointer to
         * a worker that has not yet noticed there is no room left for it.
         * Park it as a zombie exactly like every other force-completed
         * operation, instead of leaving nothing behind to catch that late
         * worker report. Do NOT count it in ar->opCountdown -- it never
         * made it into the queue as a "normal" pending operation -- but the
         * caller (Service_Read() et al.) must still learn that the
         * response is not actually done yet from the worker's point of
         * view; see persistAsyncResponse()'s ar->zombieCount handling. */
        UA_AsyncOperation_cancel(server, op, UA_STATUSCODE_BADTOOMANYOPERATIONS);
        ar->zombieCount++;
        TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        return;
    }

    /* Enqueue the asyncop in the async manager */
    TAILQ_INSERT_TAIL(&am->waitingOps, op, pointers);
    ar->opCountdown++;
    am->opsCount++;
}

/* A service callback can close its own session after earlier operations in the
 * same request have already gone asynchronous. The session is removed from the
 * server immediately, so such a response can no longer be delivered. Cancel
 * the pending operations and let the service return BadSessionClosed
 * synchronously instead of retaining an orphaned response. */
static void
cancelAsyncResponseOperations(UA_Server *server, UA_AsyncResponse *ar,
                              UA_StatusCode status) {
    UA_AsyncManager *am = &server->asyncManager;
    TAILQ_HEAD(, UA_AsyncOperation) canceledOps;
    TAILQ_INIT(&canceledOps);
    UA_AsyncOperation *op, *op_tmp;
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT ||
           op->handling.response != ar)
            continue;

        /* Unlink first. The cancellation callback may reenter the server and
         * attempt to complete this operation. */
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&canceledOps, op, pointers);
        am->opsCount--;
        UA_assert(ar->opCountdown > 0);
        ar->opCountdown--;
    }
    UA_assert(ar->opCountdown == 0);

    /* Notify only after every matching operation has been unlinked. A
     * cancellation callback may reenter and mutate the waiting queue. */
    while((op = TAILQ_FIRST(&canceledOps))) {
        TAILQ_REMOVE(&canceledOps, op, pointers);
        /* TAILQ_REMOVE does not clear the links in release builds. Avoid
         * retaining the stack-local queue head across the callback. */
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;
        UA_AsyncOperation_cancel(server, op, status);
    }
}

static UA_StatusCode
persistAsyncDirectOperation(UA_Server *server, UA_AsyncOperation *op,
                            UA_AsyncOperationType opType, void *context,
                            uintptr_t callback, UA_DateTime timeout) {
    /* Set up the async operation */
    op->asyncOperationType = opType;
    op->handling.callback.timeout = timeout;
    op->handling.callback.context = context;
    op->handling.callback.method.read = (UA_ServerAsyncReadResultCallback)callback;

    /* Not enough resources to store the async operation */
    UA_AsyncManager *am = &server->asyncManager;
    if(server->config.maxAsyncOperationQueueSize != 0 &&
       am->opsCount >= server->config.maxAsyncOperationQueueSize) {
        UA_LOG_WARNING(server->config.logging, UA_LOGCATEGORY_SERVER,
                       "Cannot create async operation: Queue exceeds limit (%d).",
                       (int unsigned)server->config.maxAsyncOperationQueueSize);
        /* The operation's callback has already run (called before this) and
         * may have handed the output pointer to a worker that has not yet
         * noticed there is no room left for it. Park it as a zombie
         * instead of deleting it out from under that worker -- like every
         * other force-completed operation, its memory can only actually be
         * freed once the worker's belated UA_Server_setAsync*Result call
         * is caught. This op was never counted in opsCount yet (that only
         * happens below, on the accepted path); count it now so
         * finalizeZombieOp()'s later decrement stays balanced. */
        UA_AsyncOperation_cancel(server, op, UA_STATUSCODE_BADTOOMANYOPERATIONS);
        am->opsCount++;
        TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        return UA_STATUSCODE_BADTOOMANYOPERATIONS;
    }

    /* Enqueue the asyncop in the async manager */
    TAILQ_INSERT_TAIL(&am->waitingOps, op, pointers);
    am->opsCount++;
    return UA_STATUSCODE_GOOD;
}

void
async_cancel(UA_Server *server, void *context, UA_StatusCode opstatus,
             UA_Boolean cancelSynchronous) {
    UA_AsyncManager *am = &server->asyncManager;
    UA_AsyncOperation *op = NULL, *op_tmp = NULL;

    /* Unlink all matching waiting operations first -- see
     * finishCanceledOp(); the same reentrancy hazard applies here. */
    TAILQ_HEAD(, UA_AsyncOperation) canceledOps;
    TAILQ_INIT(&canceledOps);
    TAILQ_FOREACH_SAFE(op, &am->waitingOps, pointers, op_tmp) {
        /* Only direct operations own a handling.callback. */
        if(op->asyncOperationType < UA_ASYNCOPERATIONTYPE_CALL_DIRECT)
            continue;
        if(op->handling.callback.context != context)
            continue;
        TAILQ_REMOVE(&am->waitingOps, op, pointers);
        TAILQ_INSERT_TAIL(&canceledOps, op, pointers);
    }

    UA_Boolean triggerReady = false;
    while((op = TAILQ_FIRST(&canceledOps))) {
        TAILQ_REMOVE(&canceledOps, op, pointers);
        op->pointers.tqe_next = NULL;
        op->pointers.tqe_prev = NULL;

        /* Cancel the operation. This sets the StatusCode and calls the
         * asyncOperationCancelCallback. */
        UA_AsyncOperation_cancel(server, op, opstatus);

        /* Call the result-callback of the local async operation. Right
         * away or in the next EventLoop iteration. Either way, the
         * operation was just marked CANCELED_WAITING_FOR_WORKER above, so
         * it must be parked as a zombie afterwards, exactly like the
         * asynchronous variant -- a worker that has not yet noticed the
         * cancellation may still be writing into its output memory,
         * and only finalizeZombieOp() (triggered by that worker's belated
         * UA_Server_setAsync*Result call) may actually free it. */
        if(cancelSynchronous) {
            directOpCallback(server, op);
            TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        } else {
            TAILQ_INSERT_TAIL(&am->readyOps, op, pointers);
            triggerReady = true;
        }
    }
    if(triggerReady)
        processReadyLater(server);

    /* All "ready" operations get processed in the next EventLoop iteration anyway */
    if(!cancelSynchronous)
        return;

    /* Process matching ready operations synchronously: notify the
     * original caller right away, same as UA_AsyncManager_processReady()
     * does, and only free the memory if the worker already acknowledged
     * the result -- otherwise park it as a zombie the same way. */
    TAILQ_FOREACH_SAFE(op, &am->readyOps, pointers, op_tmp) {
        if(op->handling.callback.context != context)
            continue;
        TAILQ_REMOVE(&am->readyOps, op, pointers);
        directOpCallback(server, op);
        if(op->status == UA_ASYNCOPERATIONSTATUS_FINISHED) {
            am->opsCount--;
            UA_AsyncOperation_delete(op);
        } else {
            TAILQ_INSERT_TAIL(&am->zombieOps, op, pointers);
        }
    }
}

void
UA_Server_cancelAsync(UA_Server *server, void *context, UA_StatusCode opstatus,
                      UA_Boolean synchronousResultCallback) {
    lockServer(server);
    async_cancel(server, context, opstatus, synchronousResultCallback);
    unlockServer(server);
}

/* A late UA_Server_setAsync*Result call for an operation that was already
 * force-completed (timeout/cancel). The result was already delivered to the
 * original caller/client. This call is only the worker's signal that it is
 * now done touching the output memory -- so it is now safe to free it. */
static void
finalizeZombieOp(UA_Server *server, UA_AsyncOperation *op) {
    UA_AsyncManager *am = &server->asyncManager;
    TAILQ_REMOVE(&am->zombieOps, op, pointers);

    if(op->asyncOperationType >= UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
        am->opsCount--;
        UA_AsyncOperation_delete(op);
        return;
    }

    /* Part of a service request. The response was already sent with the
     * cancellation status (written directly into responseSlot by
     * UA_AsyncOperation_cancel()); this late report's actual value is
     * moot, but for READ/CALL -- which own a dedicated worker buffer
     * separate from the response -- any nested data it holds (e.g. a
     * UA_Variant's array buffer) must still be freed here, since nothing
     * else will. WRITE has no separate buffer (op->output.write aliases
     * the response slot directly, see UA_AsyncOperation_cancel()) and a
     * UA_StatusCode owns no nested data anyway. */
    if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_READ_REQUEST)
        UA_DataValue_clear(op->output.read);
    else if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_REQUEST)
        UA_CallMethodResult_clear(op->output.call);

    /* Only the whole UA_AsyncResponse (shared memory) can be freed -- and
     * only once it has actually been sent. */
    UA_AsyncResponse *ar = op->handling.response;
    UA_assert(ar->zombieCount > 0);
    ar->zombieCount--;
    if(ar->zombieCount > 0)
        return;

    UA_AsyncResponse *ar2 = NULL;
    TAILQ_FOREACH(ar2, &am->zombieResponses, pointers) {
        if(ar2 != ar)
            continue;
        TAILQ_REMOVE(&am->zombieResponses, ar, pointers);
        UA_AsyncResponse_delete(ar);
        break;
    }
    /* If ar is not (yet) in zombieResponses, it has not been sent yet
     * (siblings still pending, or it is queued in readyResponses waiting
     * for the delayed callback). UA_AsyncManager_processReady will send it
     * and free it right away once it processes it, since zombieCount is
     * now 0. */
}

/********/
/* Read */
/********/

UA_Boolean
Service_Read(UA_Server *server, UA_Session *session, const void *request_, void *response_) {
    const UA_ReadRequest *request = (const UA_ReadRequest*)request_;
    UA_ReadResponse *response = (UA_ReadResponse*)response_;
    UA_LOG_DEBUG_SESSION(server->config.logging, session, "Processing ReadRequest");
    UA_LOCK_ASSERT(&server->serviceMutex);

    /* Check if the timestampstoreturn is valid */
    if(request->timestampsToReturn > UA_TIMESTAMPSTORETURN_NEITHER) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADTIMESTAMPSTORETURNINVALID;
        return true;
    }

    /* Check if maxAge is valid */
    if(request->maxAge < 0) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADMAXAGEINVALID;
        return true;
    }

    /* Check if there are too many operations */
    if(server->config.maxNodesPerRead != 0 &&
       request->nodesToReadSize > server->config.maxNodesPerRead) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADTOOMANYOPERATIONS;
        return true;
    }

    /* Check if there are no operations */
    if(request->nodesToReadSize == 0) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADNOTHINGTODO;
        return true;
    }

    /* Allocate the results array */
    UA_AsyncResponse *ar = NULL;
    UA_AsyncOperation *aopArray = NULL;
    UA_DataValue *workerBufs = NULL;
    response->results = (UA_DataValue*)
        allocateResultsArray(&UA_TYPES[UA_TYPES_DATAVALUE],
                             request->nodesToReadSize, &ar, &aopArray,
                             (void**)&workerBufs);
    if(!response->results) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADOUTOFMEMORY;
        return true;
    }
    response->resultsSize = request->nodesToReadSize;

    /* Execute the operations. Operation_Read() writes into a dedicated
     * worker-owned buffer, never directly into the response's results
     * array -- see allocateResultsArray() / UA_AsyncOperation::output. */
    for(size_t i = 0; i < request->nodesToReadSize; i++) {
        UA_Boolean done = Operation_Read(server, session, request->timestampsToReturn,
                                         &request->nodesToRead[i], &workerBufs[i]);
        if(done) {
            /* Move the value into the actual response slot: a plain
             * struct copy transferring ownership of any nested data. */
            response->results[i] = workerBufs[i];
            UA_DataValue_init(&workerBufs[i]);
        } else {
            aopArray[i].output.read = &workerBufs[i];
            aopArray[i].responseSlot.read = &response->results[i];
            persistAsyncResponseOperation(server, &aopArray[i],
                                          UA_ASYNCOPERATIONTYPE_READ_REQUEST, ar);
        }
        if(session->state == UA_SESSIONSTATE_CLOSED) {
            response->responseHeader.serviceResult = UA_STATUSCODE_BADSESSIONCLOSED;
            break;
        }
    }

    /* If async operations are pending, persist them and signal the service is
     * not done */
    if(session->state == UA_SESSIONSTATE_CLOSED && ar->opCountdown > 0)
        cancelAsyncResponseOperations(server, ar, UA_STATUSCODE_BADSESSIONCLOSED);
    if(ar->opCountdown > 0 || ar->zombieCount > 0) {
        ar->responseType = &UA_TYPES[UA_TYPES_READRESPONSE];
        persistAsyncResponse(server, session, response, ar);
    }
    return (ar->opCountdown == 0 && ar->zombieCount == 0);
}

static UA_StatusCode
readOptionalNode_async(UA_Server *server, UA_Session *session,
                       const UA_Node *node,
                       const UA_ReadValueId *operation,
                       UA_TimestampsToReturn ttr,
                       UA_ServerAsyncReadResultCallback callback,
                       void *context, UA_UInt32 timeout) {
    /* Allocate the async operation. Do this first as we need the pointer to the
     * datavalue to be stable.*/
    UA_AsyncOperation *op = (UA_AsyncOperation*)UA_calloc(1, sizeof(UA_AsyncOperation));
    if(!op)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    UA_AsyncManager *am = &server->asyncManager;
    if(server->config.maxAsyncOperationQueueSize != 0 &&
       am->opsCount >= server->config.maxAsyncOperationQueueSize) {
        UA_free(op);
        return UA_STATUSCODE_BADTOOMANYOPERATIONS;
    }

    UA_DateTime timeoutDate = UA_INT64_MAX;
    if(timeout > 0) {
        UA_EventLoop *el = server->config.eventLoop;
        const UA_DateTime tNow = el->dateTime_nowMonotonic(el);
        timeoutDate = tNow + (timeout * UA_DATETIME_MSEC);
    }

    /* Call the operation */
    UA_Boolean done = node ?
        Operation_ReadWithNode(server, session, node, ttr, operation,
                               &op->output.directRead) :
        Operation_Read(server, session, ttr, operation, &op->output.directRead);
    if(!done)
        return persistAsyncDirectOperation(server, op, UA_ASYNCOPERATIONTYPE_READ_DIRECT,
                                           context, (uintptr_t)callback, timeoutDate);

    callback(server, context, &op->output.directRead);
    UA_DataValue_clear(&op->output.directRead);
    UA_free(op);
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode
read_async(UA_Server *server, UA_Session *session,
           const UA_ReadValueId *operation, UA_TimestampsToReturn ttr,
           UA_ServerAsyncReadResultCallback callback,
           void *context, UA_UInt32 timeout) {
    return readOptionalNode_async(server, session, NULL, operation, ttr,
                                  callback, context, timeout);
}

UA_StatusCode
readWithNode_async(UA_Server *server, UA_Session *session,
                   const UA_Node *node, const UA_ReadValueId *operation,
                   UA_TimestampsToReturn ttr,
                   UA_ServerAsyncReadResultCallback callback,
                   void *context, UA_UInt32 timeout) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_assert(node != NULL);
    UA_assert(UA_NodeId_equal(&node->head.nodeId, &operation->nodeId));
    return readOptionalNode_async(server, session, node, operation, ttr,
                                  callback, context, timeout);
}

UA_StatusCode
UA_Server_read_async(UA_Server *server, const UA_ReadValueId *operation,
                     UA_TimestampsToReturn ttr, UA_ServerAsyncReadResultCallback callback,
                     void *context, UA_UInt32 timeout) {
    lockServer(server);
    UA_StatusCode res = read_async(server, &server->adminSession, operation,
                                   ttr, callback, context, timeout);
    unlockServer(server);
    return res;
}

UA_StatusCode
UA_Server_setAsyncReadResult(UA_Server *server, UA_DataValue *result) {
    lockServer(server);
    UA_AsyncManager *am = &server->asyncManager;
    UA_AsyncOperation *op = NULL;
    TAILQ_FOREACH(op, &am->waitingOps, pointers) {
        if(op->output.read == result || &op->output.directRead == result) {
            if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_READ_REQUEST) {
                /* Move the resolved value into the actual response slot,
                 * transferring ownership of any nested data. The worker
                 * buffer (result, == op->output.read) is now empty. */
                *op->responseSlot.read = *result;
                UA_DataValue_init(result);
            }
            op->status = UA_ASYNCOPERATIONSTATUS_FINISHED;
            processOperationResult(server, op);
            break;
        }
    }
    if(!op) {
        /* Late acknowledgement of an operation that already timed out/was
         * cancelled -- finalize (free) it now instead of BADNOTFOUND. */
        TAILQ_FOREACH(op, &am->zombieOps, pointers) {
            if(op->output.read == result || &op->output.directRead == result) {
                finalizeZombieOp(server, op);
                break;
            }
        }
    }
    unlockServer(server);
    return (op) ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BADNOTFOUND;
}

/*********/
/* Write */
/*********/

UA_Boolean
Service_Write(UA_Server *server, UA_Session *session,
              const void *request_, void *response_) {
    const UA_WriteRequest *request = (const UA_WriteRequest*)request_;
    UA_WriteResponse *response = (UA_WriteResponse*)response_;
    UA_assert(session != NULL);
    UA_LOG_DEBUG_SESSION(server->config.logging, session,
                         "Processing WriteRequest");
    UA_LOCK_ASSERT(&server->serviceMutex);

    if(server->config.maxNodesPerWrite != 0 &&
       request->nodesToWriteSize > server->config.maxNodesPerWrite) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADTOOMANYOPERATIONS;
        return true;
    }

    if(request->nodesToWriteSize == 0) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADNOTHINGTODO;
        return true;
    }

    /* Allocate the results array. No worker buffer is used for write --
     * UA_Server_setAsyncWriteResult() always writes the result itself,
     * under the lock, so there is no race to guard against here; see
     * UA_AsyncOperation_cancel(). */
    UA_AsyncResponse *ar = NULL;
    UA_AsyncOperation *aopArray = NULL;
    UA_StatusCode *unusedWorkerBufs = NULL;
    response->results = (UA_StatusCode*)
        allocateResultsArray(&UA_TYPES[UA_TYPES_STATUSCODE],
                             request->nodesToWriteSize, &ar, &aopArray,
                             (void**)&unusedWorkerBufs);
    if(!response->results) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADOUTOFMEMORY;
        return true;
    }
    response->resultsSize = request->nodesToWriteSize;

    /* Execute the operations */
    for(size_t i = 0; i < request->nodesToWriteSize; i++) {
        /* Ensure a stable pointer for the writevalue. Doesn't get written to,
         * just used for the lookup of the async operation later on.
         * The original writeValue might be _clear'ed before the lookup. */
        UA_AsyncOperation *aop = &aopArray[i];
        aop->context.writeValue = request->nodesToWrite[i];
        UA_Boolean done = Operation_Write(server, session, &aop->context.writeValue,
                                          &response->results[i]);
        if(!done) {
            aop->output.write = &response->results[i]; /* direct alias -- no race */
            persistAsyncResponseOperation(server, aop, UA_ASYNCOPERATIONTYPE_WRITE_REQUEST, ar);
        }
        if(session->state == UA_SESSIONSTATE_CLOSED) {
            response->responseHeader.serviceResult = UA_STATUSCODE_BADSESSIONCLOSED;
            break;
        }
    }

    /* If async operations are pending, persist them and signal the service is
     * not done */
    if(session->state == UA_SESSIONSTATE_CLOSED && ar->opCountdown > 0)
        cancelAsyncResponseOperations(server, ar, UA_STATUSCODE_BADSESSIONCLOSED);
    if(ar->opCountdown > 0 || ar->zombieCount > 0) {
        ar->responseType = &UA_TYPES[UA_TYPES_WRITERESPONSE];
        persistAsyncResponse(server, session, response, ar);
    }
    return (ar->opCountdown == 0 && ar->zombieCount == 0);
}

static UA_StatusCode
writeOptionalNode_async(UA_Server *server, UA_Session *session,
                        UA_Node *node, const UA_WriteValue *operation,
                        UA_ServerAsyncWriteResultCallback callback,
                        void *context, UA_UInt32 timeout) {
    /* Allocate the async operation. Do this first as we need the pointer to the
     * datavalue to be stable.*/
    UA_AsyncOperation *op = (UA_AsyncOperation*)UA_calloc(1, sizeof(UA_AsyncOperation));
    if(!op)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    UA_AsyncManager *am = &server->asyncManager;
    if(server->config.maxAsyncOperationQueueSize != 0 &&
       am->opsCount >= server->config.maxAsyncOperationQueueSize) {
        UA_free(op);
        return UA_STATUSCODE_BADTOOMANYOPERATIONS;
    }

    UA_DateTime timeoutDate = UA_INT64_MAX;
    if(timeout > 0) {
        UA_EventLoop *el = server->config.eventLoop;
        const UA_DateTime tNow = el->dateTime_nowMonotonic(el);
        timeoutDate = tNow + (timeout * UA_DATETIME_MSEC);
    }

    /* Call the operation */
    op->context.writeValue = *operation; /* Stable pointer */
    UA_Boolean done = node ?
        Operation_WriteWithNode(server, session, node,
                                &op->context.writeValue,
                                &op->output.directWrite) :
        Operation_Write(server, session, &op->context.writeValue,
                        &op->output.directWrite);
    if(!done)
        return persistAsyncDirectOperation(server, op, UA_ASYNCOPERATIONTYPE_WRITE_DIRECT,
                                           context, (uintptr_t)callback, timeoutDate);

    /* Done, return right away */
    callback(server, context, op->output.directWrite);
    UA_free(op);
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode
write_async(UA_Server *server, UA_Session *session,
            const UA_WriteValue *operation,
            UA_ServerAsyncWriteResultCallback callback, void *context,
            UA_UInt32 timeout) {
    return writeOptionalNode_async(server, session, NULL, operation,
                                   callback, context, timeout);
}

UA_StatusCode
writeWithNode_async(UA_Server *server, UA_Session *session,
                    UA_Node *node, const UA_WriteValue *operation,
                    UA_ServerAsyncWriteResultCallback callback,
                    void *context, UA_UInt32 timeout) {
    UA_LOCK_ASSERT(&server->serviceMutex);
    UA_assert(node != NULL);
    UA_assert(UA_NodeId_equal(&node->head.nodeId, &operation->nodeId));
    return writeOptionalNode_async(server, session, node, operation,
                                   callback, context, timeout);
}

UA_StatusCode
UA_Server_write_async(UA_Server *server, const UA_WriteValue *operation,
                      UA_ServerAsyncWriteResultCallback callback,
                      void *context, UA_UInt32 timeout) {
    lockServer(server);
    UA_StatusCode res = write_async(server, &server->adminSession, operation,
                                    callback, context, timeout);
    unlockServer(server);
    return res;
}

UA_StatusCode
UA_Server_setAsyncWriteResult(UA_Server *server,
                              const UA_DataValue *value,
                              UA_StatusCode result) {
    lockServer(server);
    UA_AsyncManager *am = &server->asyncManager;
    UA_AsyncOperation *op = NULL;
    TAILQ_FOREACH(op, &am->waitingOps, pointers) {
        if(&op->context.writeValue.value == value) {
            if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_WRITE_REQUEST)
                *op->output.write = result;
            else
                op->output.directWrite = result;
            op->status = UA_ASYNCOPERATIONSTATUS_FINISHED;
            processOperationResult(server, op);
            break;
        }
    }
    if(!op) {
        /* Late acknowledgement of an operation that already timed out/was
         * cancelled -- finalize (free) it now instead of BADNOTFOUND. The
         * result was already set (e.g. to BADTIMEOUT) when it was
         * force-completed, so it is not overwritten here. */
        TAILQ_FOREACH(op, &am->zombieOps, pointers) {
            if(&op->context.writeValue.value == value) {
                finalizeZombieOp(server, op);
                break;
            }
        }
    }
    unlockServer(server);
    return (op) ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BADNOTFOUND;
}

/********/
/* Call */
/********/

#ifdef UA_ENABLE_METHODCALLS
UA_Boolean
Service_Call(UA_Server *server, UA_Session *session,
             const void *request_, void *response_) {
    const UA_CallRequest *request = (const UA_CallRequest*)request_;
    UA_CallResponse *response = (UA_CallResponse*)response_;
    UA_LOG_DEBUG_SESSION(server->config.logging, session, "Processing CallRequest");
    UA_LOCK_ASSERT(&server->serviceMutex);

    if(server->config.maxNodesPerMethodCall != 0 &&
        request->methodsToCallSize > server->config.maxNodesPerMethodCall) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADTOOMANYOPERATIONS;
        return true;
    }

    if(request->methodsToCallSize == 0) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADNOTHINGTODO;
        return true;
    }

    /* Allocate the results array */
    UA_AsyncResponse *ar = NULL;
    UA_AsyncOperation *aopArray = NULL;
    UA_CallMethodResult *workerBufs = NULL;
    response->results = (UA_CallMethodResult*)
        allocateResultsArray(&UA_TYPES[UA_TYPES_CALLMETHODRESULT],
                             request->methodsToCallSize, &ar, &aopArray,
                             (void**)&workerBufs);
    if(!response->results) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADOUTOFMEMORY;
        return true;
    }
    response->resultsSize = request->methodsToCallSize;

    /* Execute the operations. Operation_CallMethod() writes into a
     * dedicated worker-owned buffer, never directly into the response's
     * results array -- see allocateResultsArray() / UA_AsyncOperation::output. */
    for(size_t i = 0; i < request->methodsToCallSize; i++) {
        UA_Boolean done = Operation_CallMethod(server, session, &request->methodsToCall[i],
                                               &workerBufs[i]);
        if(done) {
            /* Move the value into the actual response slot: a plain
             * struct copy transferring ownership of any nested data. */
            response->results[i] = workerBufs[i];
            UA_CallMethodResult_init(&workerBufs[i]);
        } else {
            aopArray[i].output.call = &workerBufs[i];
            aopArray[i].responseSlot.call = &response->results[i];
            persistAsyncResponseOperation(server, &aopArray[i],
                                          UA_ASYNCOPERATIONTYPE_CALL_REQUEST, ar);
        }
        if(session->state == UA_SESSIONSTATE_CLOSED) {
            response->responseHeader.serviceResult = UA_STATUSCODE_BADSESSIONCLOSED;
            break;
        }
    }

    /* If async operations are pending, persist them and signal the service is
     * not done */
    if(session->state == UA_SESSIONSTATE_CLOSED && ar->opCountdown > 0)
        cancelAsyncResponseOperations(server, ar, UA_STATUSCODE_BADSESSIONCLOSED);
    if(ar->opCountdown > 0 || ar->zombieCount > 0) {
        ar->responseType = &UA_TYPES[UA_TYPES_CALLRESPONSE];
        persistAsyncResponse(server, session, response, ar);
    }
    return (ar->opCountdown == 0 && ar->zombieCount == 0);
}

UA_StatusCode
call_async(UA_Server *server, UA_Session *session, const UA_CallMethodRequest *operation,
           UA_ServerAsyncMethodResultCallback callback, void *context,
           UA_UInt32 timeout) {
    /* Allocate the async operation. Do this first as we need the pointer to the
     * datavalue to be stable.*/
    UA_AsyncOperation *op = (UA_AsyncOperation*)UA_calloc(1, sizeof(UA_AsyncOperation));
    if(!op)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    UA_AsyncManager *am = &server->asyncManager;
    if(server->config.maxAsyncOperationQueueSize != 0 &&
       am->opsCount >= server->config.maxAsyncOperationQueueSize) {
        UA_free(op);
        return UA_STATUSCODE_BADTOOMANYOPERATIONS;
    }

    UA_DateTime timeoutDate = UA_INT64_MAX;
    if(timeout > 0) {
        UA_EventLoop *el = server->config.eventLoop;
        const UA_DateTime tNow = el->dateTime_nowMonotonic(el);
        timeoutDate = tNow + (timeout * UA_DATETIME_MSEC);
    }

    /* Call the operation */
    UA_Boolean done = Operation_CallMethod(server, session, operation,
                                           &op->output.directCall);
    if(!done)
        return persistAsyncDirectOperation(server, op, UA_ASYNCOPERATIONTYPE_CALL_DIRECT,
                                           context, (uintptr_t)callback, timeoutDate);

    /* Done, return right away */
    callback(server, context, &op->output.directCall);
    UA_CallMethodResult_clear(&op->output.directCall);
    UA_free(op);
    return UA_STATUSCODE_GOOD;
}

UA_StatusCode
UA_Server_call_async(UA_Server *server, const UA_CallMethodRequest *operation,
                     UA_ServerAsyncMethodResultCallback callback,
                     void *context, UA_UInt32 timeout) {
    lockServer(server);
    UA_StatusCode res =
        call_async(server, &server->adminSession, operation, callback, context, timeout);
    unlockServer(server);
    return res;
}

UA_StatusCode
UA_Server_setAsyncCallMethodResult(UA_Server *server, UA_Variant *output,
                                   UA_StatusCode result) {
    lockServer(server);
    UA_AsyncManager *am = &server->asyncManager;
    UA_AsyncOperation *op = NULL;
    TAILQ_FOREACH(op, &am->waitingOps, pointers) {
        if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_REQUEST) {
            if(op->output.call->outputArguments == output) {
                op->output.call->statusCode = result;
                /* Move the resolved value into the actual response slot,
                 * transferring ownership of the output arguments. The
                 * worker buffer (op->output.call) is now empty. */
                *op->responseSlot.call = *op->output.call;
                UA_CallMethodResult_init(op->output.call);
                op->status = UA_ASYNCOPERATIONSTATUS_FINISHED;
                processOperationResult(server, op);
                break;
            }
        } else if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
            if(op->output.directCall.outputArguments == output) {
                op->status = UA_ASYNCOPERATIONSTATUS_FINISHED;
                op->output.directCall.statusCode = result;
                processOperationResult(server, op);
                break;
            }
        }
    }
    if(!op) {
        /* Late acknowledgement of an operation that already timed out/was
         * cancelled -- finalize (free) it now instead of BADNOTFOUND. */
        TAILQ_FOREACH(op, &am->zombieOps, pointers) {
            if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_REQUEST) {
                if(op->output.call->outputArguments == output) {
                    finalizeZombieOp(server, op);
                    break;
                }
            } else if(op->asyncOperationType == UA_ASYNCOPERATIONTYPE_CALL_DIRECT) {
                if(op->output.directCall.outputArguments == output) {
                    finalizeZombieOp(server, op);
                    break;
                }
            }
        }
    }
    unlockServer(server);
    return (op) ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BADNOTFOUND;
}
#endif
