Ping Pong dialogue between a worker process and a broker process. 
-------------



::

## Sockets
* worker.frontend (DEALER) <-----> broker.backend (ROUTER)

## Constants

* BROKER_TIMEOUT: 
* N_TIMEOUTS:
* WOKER_EXPIRE_SEONDS:


## Message flows

Worker READY  --->          Broker

Worker PING   --->          Broker<br/>
Worker        <---  PONG    Broker

Worker        <---  job     Broker
Worker result --->          Broker

Worker        <---  EXIT    Broker


## Worker actions

* Worker ready; send ["", READY]; The worker process sends ["", READY] when it is ready. 
* Worker heartbeat; send ["". PING]; Every second, the worker sends a ["", PING] to let the broker know that it's alive. 
* Worker recieve job; recv ["", job]; When the worker receives a message that is not ["", PONG] or ["", EXIT] it will assume that the message is a job task and process the job. 
* Worker send result; send ["", result]; When job processing is complete it will send the result

* Expire broker; none; If the worker has received no messages for N_TIMEOUTS consecutive periods of BROKER_TIMEOUT the worker will close, re-initialize frontend and then send ["", READY].
* Worker exit; none; If the worker receives ["", EXIT] it will exit the process. 



## Broker actions
<!-- Broker ready: The broker will have no registered workers, so it does nothing. -->
* Recv READY: Register worker: When broker receives READY on BACKEND it adds worker to queue and immediately sends PONG.
* Recv PING: PONG: When the broker receives a PING on BACKEND it immediately responds with [worker_id, '', PONG]. If worker_id is not in the ready queue, worker_id is added to the ready queue.


* Update worker expiration: When the broker receives any message from a worker it updates the timeout the worker to current time plus WORKER_EXPIRE_SECONDS
* Timeout worker: When the broker has not received any messages from a worker for WORKER_EXPIRE_SECONDS it removes the worker from the ready queue.
* Kill worker: Broker removes worker from queue and sends [worker_id, '', EXIT]. 
* Assign job: Broker removes worker from queue and sends [worker_id, '', job] 

## Notes
* The dialog between worker and broker is **asynchronous**.
* When the broker send EXIT to a worker, the worker does not send an acknowledgement. Hence the broker does not contain state infomation about the worker.
* While the worker is working, it does not send out PONG messages. 

* If the worker is one that does heavy/latency prone jobs, then it should launch a separate thread to do that actual work, so that it can continue sending PONG messages.
* If 

* When a worker expires the broker does not send EXIT. This is because, if 


* Usually the broker will keep a list of live workers, and append the IDENTITY of the worker to that list.
* When the broker receives PING or READY it responds with PONG

* If the ping process sends a *ping*, the pong processes responds with a
  *pong*.
* The number of pings (and pongs) is counted. The current ping count is
  sent with each message.

::

    WorkerProc      BrokerProc
     [DEALER] <-----> [ROUTER]
              <--2---
              ---3-->


    1 IN : ['', 'ping']
    1 OUT: ['ping, count']

    2 IN : ['pong, count']
    2 OUT: ['pong, count']


    Broker starts
