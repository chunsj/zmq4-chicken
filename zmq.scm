(module zmq

(zmq-default-context zmq-io-threads zmq-version
 make-context terminate-context context?
 make-socket socket? close-socket bind-socket connect-socket
 socket-option-set! socket-option socket-fd
 send-message receive-message receive-message*
 make-poll-item poll poll-item-socket
 poll-item-fd poll-item-in? poll-item-out? poll-item-error?)

(import (except chicken errno) scheme foreign)
(use lolevel foreigners srfi-1 srfi-18 srfi-13 data-structures)

(foreign-declare "#include <zmq.h>")
(foreign-declare "#include <errno.h>")

(define-record context pointer sockets)
(define-foreign-type context c-pointer)

(define-foreign-type message (c-pointer "zmq_msg_t"))

(define-record socket pointer mutex message)
(define-foreign-type socket c-pointer)

(define-foreign-enum-type (socket-type int)
  (socket-type->int int->socket-type)
  ((pair) ZMQ_PAIR)
  ((pub) ZMQ_PUB)
  ((sub) ZMQ_SUB)
  ((req) ZMQ_REQ)
  ((rep) ZMQ_REP)
  ((dealer) ZMQ_DEALER)
  ((router) ZMQ_ROUTER)
  ((pull) ZMQ_PULL)
  ((push) ZMQ_PUSH)
  ((xpub) ZMQ_XPUB)
  ((xsub) ZMQ_XSUB)
  ((stream) ZMQ_STREAM))

(define-foreign-enum-type (socket-option int)
  (socket-option->int int->socket-option)
  ((sndhwm) ZMQ_SNDHWM)
  ((rcvhwm) ZMQ_RCVHWM)
  ((affinity) ZMQ_AFFINITY)
  ((identity) ZMQ_IDENTITY)
  ((subscribe) ZMQ_SUBSCRIBE)
  ((unsubscribe) ZMQ_UNSUBSCRIBE)
  ((rate) ZMQ_RATE)
  ((recovery-ivl) ZMQ_RECOVERY_IVL)
  ((sndbuf) ZMQ_SNDBUF)
  ((rcvbuf) ZMQ_RCVBUF)
  ((rcvmore) ZMQ_RCVMORE)
  ((fd) ZMQ_FD))

(define socket-options
  '((integer hwm swap affinity rate recovery-ivl sndbuf rcvbuf)
    (boolean rcvmore mcast-loop)
    (string subscribe unsubscribe identity)))

(define-foreign-enum-type (socket-flag int)
  (socket-flag->int int->socket-flag)
  ((noblock zmq/noblock) ZMQ_NOBLOCK)
  ((sndmore zmq/sndmore) ZMQ_SNDMORE))

(define-foreign-enum-type (poll-flag short)
  (poll-flat->int short->poll-int)
  ((in zmq/pollin) ZMQ_POLLIN)
  ((out zmq/pollout) ZMQ_POLLOUT)
  ((err zmq/pollerr) ZMQ_POLLERR))

(define-record poll-item pointer socket in out)
(define-foreign-record-type (poll-item zmq_pollitem_t)
  (constructor: make-foreign-poll-item)
  (destructor: free-foreign-poll-item)
  (socket socket %poll-item-socket %poll-item-socket-set!)
  (int fd %poll-item-fd %poll-item-fd-set!)
  (short events %poll-item-events %poll-item-events-set!)
  (short revents %poll-item-revents %poll-item-revents-set!))

(define-foreign-enum-type (errno int)
  (errno->int int->errno)
  ((again) EAGAIN)
  ((term) ETERM))

;; helpers

(define (zmq-error location)
  (let ((errno (foreign-value errno int)))
    (error location
           ((foreign-lambda c-string zmq_strerror int) errno)
           errno)))

(define (errno)
  (foreign-value errno errno))

(define (type-error value expected-type)
  (error (format "expected ~A, got" value) expected-type))

(define (zmq-version)
  (let-location ((major int) (minor int) (patch int))
    ((foreign-lambda void zmq_version (c-pointer int) (c-pointer int) (c-pointer int))
     (location major) (location minor) (location patch))
    (list major minor patch)))

;; contexts

(define zmq-io-threads (make-parameter 1))

(define zmq-default-context (make-parameter #f))

(define (zmq-default-context/initialize)
  (or (zmq-default-context)
      (begin (zmq-default-context (make-context (zmq-io-threads)))
             (zmq-default-context))))

(define %make-context make-context)

(define (make-context io-threads)
  (let ((c (%make-context ((foreign-lambda context zmq_init int) io-threads)
                          (make-mutex))))
    (if (not (context-pointer c))
        (zmq-error 'make-context)
        (begin
          (mutex-specific-set! (context-sockets c) '())
          (set-finalizer! c (lambda (c)
                              (for-each close-socket (mutex-specific (context-sockets c)))
                              (terminate-context c)))))))

(define (terminate-context ctx)
  (or (zero? ((foreign-lambda int zmq_term context)
              (context-pointer ctx)))
      (zmq-error 'terminate-context)))

;; messages

(define (initialize-message message #!optional data)
  (if (zero? (if data
                 (begin
                  (unless (or (string? data) (blob? data))
                    (type-error data "string or blob"))
                  (let* ((len (number-of-bytes data))
                         (cdata (allocate len)))
                    ((foreign-lambda void "C_memcpy" c-pointer scheme-pointer int)
                     cdata data len)
                    ((foreign-lambda int
                                     zmq_msg_init_data
                                     message
                                     c-pointer
                                     unsigned-int
                                     c-pointer
                                     c-pointer)
                     message
                     cdata
                     len
                     (foreign-value "C_free" c-pointer)
                     #f)))
                 ((foreign-lambda int zmq_msg_init message) message)))
      message
      (zmq-error 'initialize-message)))

(define (close-message message)
  (or (zero? ((foreign-lambda int zmq_msg_close message) message))
      (zmq-error 'close-message)))

(define (message-size message)
  ((foreign-lambda unsigned-integer zmq_msg_size message) message))

(define (message-data message type)
  (let* ((size (message-size message))
         (ptr ((foreign-lambda c-pointer zmq_msg_data message) message)))

    (cond ((symbol? type)
           (case type
             ((string)
              (let ((str (make-string size)))
                (move-memory! ptr str size)
                str))
             ((blob)
              (let ((blob (make-blob size)))
                (move-memory! ptr blob size)
                blob))
             (else (error 'message-data "invalid message data type" type))))
          ((procedure? type)
           (type ptr size))
          (else (error 'message-data "invalid message data type" type)))))

;; sockets

(define %make-socket make-socket)

(define (make-socket type #!optional (context (zmq-default-context/initialize)))
  (let ((sp ((foreign-lambda socket zmq_socket context socket-type)
             (context-pointer context) type)))
    (if (not sp)
        (zmq-error 'make-socket)
        (let ((m (context-sockets context))
              (s (%make-socket sp
                               (make-mutex)
                               (allocate (foreign-value "sizeof(zmq_msg_t)" int)))))

          (mutex-lock! m)
          (mutex-specific-set! m (cons sp (mutex-specific m)))
          (mutex-unlock! m)
          (set-finalizer! s (lambda (s)
                              (free (socket-message s))
                              (close-socket s)))))))

(define (close-socket socket)
  (let ((sp (cond ((socket? socket) (socket-pointer socket))
                  ((pointer? socket) socket)
                  (else (type-error socket 'socket)))))

    (when sp
      (if (zero? ((foreign-lambda int zmq_close socket) sp))
          (when (socket? socket) (socket-pointer-set! socket #f))
          (zmq-error 'close-socket)))))

(define (bind-socket socket endpoint)
  (or (zero? ((foreign-lambda int zmq_bind socket c-string)
              (socket-pointer socket)
              endpoint))
      (zmq-error 'bind-socket)))

(define (connect-socket socket endpoint)
  (or (zero? ((foreign-lambda int zmq_connect socket c-string)
              (socket-pointer socket)
              endpoint))
      (zmq-error 'connect-socket)))

;; integer64 is used instead of unsigned-integer64 for uint64_t
;; options since the latter has only been added to the experimental
;; branch recently. Also, we must use foreign-lambda* to be able to
;; pass in integer64 values because let-location doesn't accept
;; integer64 (also fixed in experimental)

(define (socket-option-set! socket option value)
  (or (zero? (case option
               ((hwm affinity sndbuf rcvbuf swap rate recovery-ivl mcast-loop)
                (if (integer? value)
                    ((foreign-safe-lambda* int
                         ((scheme-object error)
                          (scheme-object error_location)
                          (socket socket)
                          (socket-option option)
                          (integer64 value))
                       "size_t size = sizeof(value);

                                           if (0 == zmq_setsockopt(socket, option, &value, size)) {
                                             C_return(0);
                                           } else {
                                             C_save(error_location);
                                             C_callback(error, 1);
                                           }")
                     zmq-error 'socket-option-set! (socket-pointer socket) option value)
                    (type-error value 'integer)))

               ((identity subscribe unsubscribe)
                (if (string? value)
                    (or ((foreign-lambda int zmq_setsockopt socket socket-option c-string unsigned-int)
                         (socket-pointer socket) option value (number-of-bytes value))
                        (zmq-error 'socket-option-set!))
                    (type-error value 'string)))

               (else (error (format "unknown socket option: ~A" option)))))
      (zmq-error 'socket-option-set!)))

(define-syntax %socket-option
  (lambda (e r c)
    (let ((location (second e))
          (f-type (third e))
          (c-type (fourth e))
          (socket (fifth e))
          (option (sixth e)))
      `((,(r 'foreign-safe-lambda*) ,f-type ((scheme-object error)
                                             (scheme-object error_location)
                                             (socket socket)
                                             (socket-option option))
         ,(string-append c-type " value;
                                  size_t size = sizeof(value);

                                  if (0 == zmq_getsockopt(socket, option, &value, &size)) {
                                    C_return(value);
                                  } else {
                                    C_save(error_location);
                                    C_callback(error, 1);
                                  }"))
        ,(r 'zmq-error) ,location (,(r 'socket-pointer) ,socket) ,option))))

(define socket-option
  (let ((identity (make-string 255)))
    (lambda (socket option)
      (cond ((eq? option 'identity)
             (let-location ((size int))
               (if (zero? ((foreign-lambda int zmq_getsockopt socket socket-option scheme-pointer c-pointer)
                           (socket-pointer socket) option identity (location size)))
                   (substring identity 0 size)
                   (zmq-error 'socket-option))))

            ((memq option (alist-ref 'integer socket-options))
             (%socket-option 'socket-option integer64 "uint64_t" socket option))

            ((memq option (alist-ref 'boolean socket-options))
             (%socket-option 'socket-option bool "int64_t" socket option))

            (else
             (error (format "socket option ~A is not retrievable" option)))))))

(define (socket-fd socket)
  (%socket-option 'socket-fd int "int" socket 'fd))

;; communication

(define (send-message socket data #!key non-blocking send-more)
  (mutex-lock! (socket-mutex socket))
  (let* ((message (initialize-message (socket-message socket) data))
         (result ((foreign-lambda int zmq_msg_send message socket int)
                  (socket-pointer socket)
                  message
                  (bitwise-ior (if non-blocking zmq/noblock  0)
                               (if send-more zmq/sndmore 0)))))

    (close-message message)
    (mutex-unlock! (socket-mutex socket))
    (or (zero? result) (zmq-error 'send-message))))


(define (receive-message socket #!key non-blocking (as 'string))
  (mutex-lock! (socket-mutex socket))
  (let* ((message (initialize-message (socket-message socket)))
         (result ((foreign-lambda int zmq_msg_recv message socket int)
                  (socket-pointer socket)
                  message
                  (if non-blocking zmq/noblock 0))))


    (if (zero? result)
        (let ((data (message-data message as)))
          (mutex-unlock! (socket-mutex socket))
          (close-message message)
          data)
        (begin
          (mutex-unlock! (socket-mutex socket))
          (close-message message)
          (if (memq (errno) '(again term))
              #f
              (zmq-error 'receive-message))))))

(define (receive-message* socket #!key (as 'string))
  (or (receive-message socket non-blocking: #t as: as)
      (begin
        (thread-wait-for-i/o! (socket-fd socket) #:input)
        (receive-message* socket as: as))))

;; polling

(define %make-poll-item make-poll-item)

(define (make-poll-item socket/fd #!key in out)
  (let ((item (%make-poll-item (make-foreign-poll-item)
                               (and (socket? socket/fd) socket/fd)
                               in out)))
    (if (socket? socket/fd)
        (%poll-item-socket-set! (poll-item-pointer item) (socket-pointer socket/fd))
        (%poll-item-fd-set! (poll-item-pointer item) socket/fd))

    (%poll-item-events-set! (poll-item-pointer item)
                            (bitwise-ior (if in zmq/pollin 0)
                                         (if out zmq/pollout 0)))

    (%poll-item-revents-set! (poll-item-pointer item) 0)

    (set-finalizer! item (lambda (i)
                           (free-foreign-poll-item (poll-item-pointer i))))))

(define (poll-item-fd item)
  (%poll-item-fd (poll-item-pointer item)))

(define (poll-item-revents item)
  (%poll-item-revents (poll-item-pointer item)))

(define (poll-item-in? item)
  (not (zero? (bitwise-and zmq/pollin (poll-item-revents item)))))

(define (poll-item-out? item)
  (not (zero? (bitwise-and zmq/pollout (poll-item-revents item)))))

(define (poll-item-error? item)
  (not (zero? (bitwise-and zmq/pollerr (poll-item-revents item)))))

(define %poll-sockets
  (foreign-safe-lambda* int
                        ((scheme-object poll_item_ref)
                         (unsigned-int length)
                         (long timeout))
                        "zmq_pollitem_t items[length];
                         zmq_pollitem_t *item_ptrs[length];
                         int i;

                         for (i = 0; i < length; i++) {
                           C_save(C_fix(i));
                           item_ptrs[i] = (zmq_pollitem_t *)C_pointer_address(C_callback(poll_item_ref, 1));
                         }

                         for (i = 0; i < length; i++) {
                           items[i] = *item_ptrs[i];
                         }

                         int rc = zmq_poll(items, length, timeout);

                         if (rc != -1) {
                           for (i = 0; i < length; i++) {
                             (*item_ptrs[i]).revents = items[i].revents;
                           }
                         }

                         C_return(rc);"))

(define (poll poll-items timeout/block)
  (if (null? poll-items)
      (error 'poll "null list passed for poll-items")
      (let ((result (%poll-sockets (lambda (i)
                                     (poll-item-pointer (list-ref poll-items i)))
                                   (length poll-items)
                                   (case timeout/block
                                     ((#f) 0)
                                     ((#t) -1)
                                     (else timeout/block)))))
        (if (= result -1)
            (zmq-error 'poll)
            result))))
)
