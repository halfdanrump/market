Ping Pong dialogue between a worker process and a broker process. 

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

Worker        <---  job     Broker<br/>
Worker result --->          Broker

Worker        <---  EXIT    Broker


## Worker

* Worker ready; send ["", READY]; The worker process sends ["", READY] when it is ready. 
* Worker heartbeat; send ["", PING]; Every second, the worker sends a ["", PING] to let the broker know that it's alive. 
* Worker send result; send ["", result]; When job processing is complete it will send the result

* Worker recieve job; recv ["", job]; When the worker receives a message that is not ["", PONG] or ["", EXIT] it will assume that the message is a job task and process the job. 


* Worker exit; recv ["", EXIT]; If the worker receives exit message it will exit the process. * Expire broker; none; If the worker has received no messages for N_TIMEOUTS consecutive periods of BROKER_TIMEOUT the worker will close, re-initialize frontend and then send ["", READY].




### Broker
* Recv READY; register worker; when broker receives READY on BACKEND it adds worker to queue and immediately sends PONG.
* Recv PING; PONG; When the broker receives a PING on BACKEND it immediately responds with [worker_id, '', PONG]. If worker_id is not in the ready queue, worker_id is added to the ready queue.
* Kill worker; send [worker_id, "", EXIT]; broker removes worker from queue and sends exit message. 
* Assign job; send [worker_id, "", job]; broker removes worker from queue and sends [worker_id, '', job] 
* Update worker expiration: When the broker receives any message from a worker it updates the timeout the worker to current time plus WORKER_EXPIRE_SECONDS
* Timeout worker: When the broker has not received any messages from a worker for WORKER_EXPIRE_SECONDS it removes the worker from the ready queue.

## Notes
* The dialog between worker and broker is **asynchronous**.
* When the broker send EXIT to a worker, the worker does not send an acknowledgement. Hence the broker does not contain state infomation about the worker.
* While the worker is working, it does not send out PONG messages. 