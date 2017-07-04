using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;
namespace MultiThreadingFramework
{
    class MultiThreadWorker
    {
        private Thread t;
        private String threadName;
        private bool stopping = false;
        private bool running = false;
        private static int smallSleepTime = 11;
        private int sleepTime = 20;
        private static int sleepAdj = 17;
        private static int almost1Second = 987;
        public bool isProcessing = false;
        public long currentTime = CommonConstants.currentTimeMillis();

        ///////////////////////////////////////////////
        // QUEUES - Thread safe - shared between all workers and controller
        private ConcurrentQueue<InputDTO> workerQueue = null;
        private ConcurrentQueue<ServiceResult> resultQueue = null;

        ///////////////////////////////////////////////
        /// work is done here
        /// 

        public void doWork()
        {
		    Random rand = new Random();
		    InputDTO dto = null;
		    ServiceResult res = null;
		    OutputDTO outDTO = null;
		    running = true;
            Console.WriteLine(CommonConstants.currentTimeExtended() + ": Running " +  threadName );
	        try {
	            currentTime = CommonConstants.currentTimeMillis();
	    	    while (!stopping) {
                    int pauseTime = rand.Next(51)+180; // random web request time 180..230
	                if (!workerQueue.IsEmpty) {
	                    // prevent blocking loop by checking Q length first
                        dto = getFromWorkerQ();   // may return null
                        if (dto != null) {
	                    // simulate processing = Let the thread sleep for a while.
    	                    Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread: " + threadName + ", row " + dto.RowNbr+ ", Qlen = " + getQLength() +
    	                                       ", working for " + pauseTime);
        	                Thread.Sleep(pauseTime);
        	                // send back the results
        	                outDTO = new OutputDTO(dto);
        	                res = new ServiceResult(CommonConstants.statusSuccess, outDTO);
        	                putToResultQ(res);
                        }
	                }
	                if (getQLength() == 0) {  
	            	    long endTime = CommonConstants.currentTimeMillis();
	            	    int deltaT = (int) (endTime - currentTime);
                        if (deltaT < 100) { // less than a tenth second has elapsed
                            Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread: " + threadName + ", Qlen = " + getQLength() + ", sleeping for " + sleepAdj);
                            Thread.Sleep(sleepAdj);
                        } else if (deltaT < almost1Second) { // less than a second has elapsed
                            sleepTime = 1000 - deltaT - sleepAdj;
                            if (sleepTime < 0) {
                                sleepTime = smallSleepTime;
                            }
	            		    Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread: " + threadName + ", Qlen = " + getQLength() + ", sleeping for " + sleepTime);
            		        Thread.Sleep(sleepTime);
	            	    } else {
	            		    Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread: " + threadName + ", Qlen = " + getQLength() + ", elapsed " + deltaT + ", sleeping for " + smallSleepTime);
	            		    Thread.Sleep(smallSleepTime);
	            	    }
            		    //currentTime = System.currentTimeMillis();
	                }
	    	    }  
	         } catch (ThreadInterruptedException e) {
	             Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread " +  threadName + " interrupted.");
	         }
             Console.WriteLine(CommonConstants.currentTimeExtended() + ": Thread " + threadName + " exiting.");
	         running = false;
        }


        private int getQLength() {
            lock (workerQueue) {
                return workerQueue.Count();
            }
        }

        private InputDTO getFromWorkerQ() {
            lock (workerQueue) {
                if (workerQueue.Count() > 0)
                {
                    // take() and poll(timeout) can block
                    // the timeout is erratic - often at least 5 to 10 times the value
                    // polling can cause a blocking loop that can run for up to 500ms for all workers
                    // by checking the Q length before trying take(), we generally 
                    // have sufficient rows to satisfy all workers.
                    long t1 = CommonConstants.currentTimeMicros();
                    InputDTO dto;
                    bool gotit = workerQueue.TryDequeue(out dto);
                    long t2 = CommonConstants.currentTimeMicros() - t1;
                    if (t2 > 10)
                    { // 10th of milisecond
                        Console.WriteLine(CommonConstants.currentTimeExtended() + ": " + threadName + " dequeue timeout=" + t2 + " micros");
                    }
                    if (gotit)
                    {
                        isProcessing = true;
                        return dto;
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }
        }
    
        private void putToResultQ(ServiceResult sr){
            lock (resultQueue) {
                resultQueue.Enqueue(sr);
                isProcessing = false;
            }
        }
    

        public void start()  {
    	    Console.WriteLine(CommonConstants.currentTimeExtended() + ": Starting " +  threadName);
	        if (t == null) {
                t = new Thread(doWork);
                t.Start ();
	        }
        }

        public void stop()
        {
            stopping = true;
        }
        public void stopNow()
        {
            stopping = true;
            t.Interrupt();
        }

        public bool isRunning()
        {
            return running;
        }

        public void setRunning(bool running)
        {
            this.running = running;
        }
    
        
        public MultiThreadWorker(String name, ConcurrentQueue<InputDTO> wQ, ConcurrentQueue<ServiceResult> rQ)
        {
		    threadName = name;
            Console.WriteLine(CommonConstants.currentTimeExtended() + ": Creating " + threadName);		
		    workerQueue = wQ;
		    resultQueue = rQ;
        }

    }
}
