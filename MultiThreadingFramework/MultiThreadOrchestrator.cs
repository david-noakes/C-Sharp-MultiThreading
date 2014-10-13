using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Concurrent;

namespace MultiThreadingFramework
{
    class MultiThreadOrchestrator
    {
        private String serviceStopFile = "/opt/local/geocode/stop";
        private bool simulateDB = true;
        private String userDir;
        private String simulFileName;
        System.IO.StreamReader dbOutput = null;
        private bool workersStarted = false;
        List<MultiThreadWorker> workers;
        private bool tooManyWorkers = false;
        private bool needMoreWorkers = false;

        ///////////////////////////////////////////////
        // QUEUES - Thread safe
        private ConcurrentQueue<InputDTO> workerQueue = null;
        private ConcurrentQueue<ServiceResult> resultQueue = null;




        // ////////////////////////////////////////////
        private static int maxThreads = 11;
        private static int minThreads = 3;
        private static int nbrThreadsInit = 4;
        private bool stopping = false;
        long updateTimestamp;
        private static int requestsPerSecond = 22;
        private static int minNbrToRead = minThreads;
        // ////////////////////////////////////////////
        private static int processingTooFastTime = 700; //if we clear the Q in less than this time, kill off a worker
        private static int processingTooSlowTime = 900; //if we take longer than this, we need another worker

        // Queue loading pause time 
        private static int queueLoadingPauseTimeValue = 2;
        // housekeeping loop sleep timer - for processing our Q and waiting for 1 second to elapse
        private static int housekeepingSleepTimeValue = 13;
        // pseudo processsing loop sleep timer - for processing 1 Q item 
        private static int pseudoProcessingSleepTimeValue = 7;
        //pause after loading queue for workers to do work
        private static int waitForWorkersTime = Math.Max(300, 350 - (queueLoadingPauseTimeValue * nbrThreadsInit));


        // Long 'Sleep' timer when there are no records to process
        private static int maxShutdownTimeValue = 5000;
        // Timer increase value
        private static int increaseTimeValue = 100;
        private static int almost1Second = 1000 - housekeepingSleepTimeValue + 1;






	    public void Orchestrate() {
		    performInit();
		    openDBConnection();
		    int counter = 0;
		    int burst = 0;
		    int wqLen = 0;

		    // ////////////////////////////////////////////
	        workerQueue = new ConcurrentQueue<InputDTO>() ;
	        resultQueue = new ConcurrentQueue<ServiceResult>();
		    workers = new List<MultiThreadWorker>(maxThreads);
		    for (int i = 0; i < nbrThreadsInit; i++) {
			    workers.Add(new MultiThreadWorker("Worker "+ i, workerQueue, resultQueue));
		    }

		    // ////////////////////////////////////////////
		
		    long currentTime  = CommonConstants.currentTimeMillis();
            long startTime  = currentTime;
		    int timeDelta = 0;
		    int saveTimeDelta = 0;

		    while (!stopping) {
                // we should come through here once per second
		        // almost1Second is exactly that - if we construct the loop for exactly 1 second, 
		        // we go past that value by as much as 35 ms
		        // recalculate it here, because we might adjust the housekeepingSleepTimeValue
		        almost1Second = 1000 - housekeepingSleepTimeValue +1;

		        // read dbOutput requestsPerSecond rows at a time, and apportion to workers
		        // if the workerQueue contains rows, real less rows to ensure the queue total 
		        // will be the configured batch size
			    try {
				    List<String> rows = performQuery(wqLen);  // could set stopping = true
				
				    Console.WriteLine("read "+rows.Count + " qLen = " + wqLen);
				    burst = rows.Count + getWorkerQLength(); // how many we are trying to process this second
				    if (StopFileExists(serviceStopFile)) {
					    stopping = true;   // only set flag to allow orderly shutdown
				    } 
                    for (int i = 0; i < workers.Count; i++) {
                        workers.ElementAt(i).currentTime = currentTime; 
                    }
				    if (rows.Count > 0) {
				        for (int i = 0; i<rows.Count;i++) {
				            counter++;
					        InputDTO dto = new InputDTO((String) rows.ElementAt(i), counter);
					        putToWorkerQ(dto);
					        if (i>0 && i % workers.Count == 0) {
					            Thread.Sleep(queueLoadingPauseTimeValue);
					        }
					    }
				    }
                    currentTime = CommonConstants.currentTimeMillis(); 
                    saveTimeDelta = 0;
				    if (!workersStarted) {
				        // set startTime again for closer approx of actual time to process batch
				        startTime = CommonConstants.currentTimeMillis();
					    for (int i = 0; i < workers.Count; i++) {
						    workers.ElementAt(i).start();
					    }
					    workersStarted = true;
				    } else {
                        for (int i = 0; i < workers.Count; i++) {
                            // start any new ones
                            if (!workers.ElementAt(i).isRunning()) {
                                workers.ElementAt(i).start();
                            } else {
                                // reset currentTime for workers
                                // they can then sleep for the balance of the second if queue goes empty
                                workers.ElementAt(i).currentTime = currentTime; 
                            }
                        }
				    }
				
				    // allow the workers to do some work
				    Thread.Sleep(waitForWorkersTime); // this should not be made too small
				
				    timeDelta = (int) (CommonConstants.currentTimeMillis() - currentTime);
				
				    //************************************//
				    while (timeDelta < almost1Second) {
	                    // this is our housekeeping loop
	                    // process the output from the workers, update the database
	                    if (allWorkersIdle()) {
	                        if (saveTimeDelta == 0) {
	                            saveTimeDelta = timeDelta; // used to calculate burst rate
	                        }
	                        if (!stopping && !tooManyWorkers  && timeDelta < processingTooFastTime) {
    	                        tooManyWorkers = true;
    	                        Console.WriteLine("too many workers @ "+timeDelta);
	                        }
	                    }
				        if (getResultQLength() > 0) {
				            // process 1 entry in our input q
				            processResultQItem();
				        } else {
				           Thread.Sleep(housekeepingSleepTimeValue); 
				        }
				        timeDelta = (int) (CommonConstants.currentTimeMillis() - currentTime);
				    }
                    //************************************//

				    // almost 1 second has elapsed.
				
				    if (stopping) {  // not a panic abort, but orderly shutdown
					    while (!allWorkersIdle() || getResultQLength() > 0) {
	                        if (getResultQLength() > 0) {
	                            // process 1 entry in our input q
	                            processResultQItem();
	                        } else {
	                           Thread.Sleep(housekeepingSleepTimeValue); 
	                        }
					    }
					    // we get here, all Qs are empty
					    // we can now shut down the workers
                        updateTimestamp = CommonConstants.currentTimeMillis();
                        long shutdownAbort = updateTimestamp + maxShutdownTimeValue;
					    for (int i = 0; i < workers.Count; i++) {
						    workers.ElementAt(i).stop();
					    }
					    Thread.Sleep(300);
					    bool threadsStillRunning = true;
					    while (threadsStillRunning) {
					        if (CommonConstants.currentTimeMillis() > shutdownAbort) {
        					    for (int i = 0; i < workers.Count; i++) {
        					        if (workers.ElementAt(i).isRunning()) {
        					            workers.ElementAt(i).stopNow();
        					        }
        					    }
					        } else {
					            threadsStillRunning = false;
                                for (int i = 0; i < workers.Count; i++) {
                                    if (workers.ElementAt(i).isRunning()) {
                                        threadsStillRunning = true;
                                    }
                                }
					        }
                            if (threadsStillRunning) {
                                Thread.Sleep(100);
                            }
					    }
				    } else { 
				        // not stopping, almost 1 second elapsed.
				        // do we need more or less workers?
				        wqLen = getWorkerQLength();
    				    burst -= wqLen;
    				    if (saveTimeDelta !=0) {
    				        Console.WriteLine("processed " + burst + " items in " + saveTimeDelta + " ms, loop time="+timeDelta);
    				    } else {
    				        Console.WriteLine("processed " + burst + " items in " + timeDelta + " ms");
    				        saveTimeDelta = timeDelta;
    				    }
                        if (wqLen > 0 || saveTimeDelta > processingTooSlowTime ) {
                            needMoreWorkers = true;
                            Console.WriteLine("more workers needed Q = " + wqLen + ", tDelta="+saveTimeDelta);
                        }
    				    if (needMoreWorkers) {
    				        int nbrNeeded = wqLen / workers.Count;
    				        if (nbrNeeded == 0) {
    				            nbrNeeded = 1;
    				        }
    				        for (int i = 0; i < nbrNeeded; i++) {
        				        if (workers.Count < maxThreads) {
        				            // do not start them until we give them work
    				                int n = workers.Count;
    				                MultiThreadWorker mtw = new MultiThreadWorker("Worker "+ n, workerQueue, resultQueue); 
                                    workers.Add(mtw);
        				        }
    				        }
    				        needMoreWorkers = false;
    				        tooManyWorkers = false;
    				    }
    				    if (tooManyWorkers) {
    				        if (workers.Count > minThreads  && saveTimeDelta > 0 && wqLen == 0) {
    				            // remove 1 worker
    				            int n=workers.Count - 1;
        				        MultiThreadWorker mtw = workers.ElementAt(n);
    				            workers.RemoveAt(n);
        				        mtw.stop(); // this allows it to shut down in an orderly fashion
        				                    // and return its result if it is processing
    				        }
    				        tooManyWorkers = false;
    				    }
				    }
			    } catch (ThreadInterruptedException ex) {
				    Console.WriteLine("Thread was interrupted : " + ex.Message);
			    }
		    }
		    closeDBConnection(StopFileExists(serviceStopFile));
		    if (StopFileExists(serviceStopFile)) {
		        System.IO.File.Delete(serviceStopFile );
		    }

	        long deltaT = updateTimestamp - startTime;
	        double decSecs = ((double) deltaT) / 1000;
	        int seconds = (int) (deltaT / 1000);
	        double average = Math.Floor(((double) counter * 1000) / ((double) deltaT)*100)/100;		// per second 2 decimal places
            Console.WriteLine(" #### - Stopping - processed " + counter + " requests in " + decSecs + " seconds" +
                    " ==> average = " + average + " per second");
	    }


        private void performInit()
        {
            Console.WriteLine("##### Starting Application");

            //readConfigFile(configFilePath);

            // Update 'urlSuffix' value to include 'clientID'
            //urlSuffix = urlSuffix + "&client=" + clientID;

            // Create Service 'StopFile'
            //File stopFile = new File(serviceStopFile);

            //return stopFile;
        }


        private void openDBConnection()
        {
            if (simulateDB)
            {
                try
                {
                    userDir = Environment.CurrentDirectory;
                    simulFileName = userDir + "/../../Resources/scripts/address_data.csv";
                    //simulFile = new File(simulFileName);
                    dbOutput = new System.IO.StreamReader(simulFileName);

                }
                catch (System.IO.FileNotFoundException e)
                {
                    // TODO Auto-generated catch block
                    Console.WriteLine(e.StackTrace);
                }
            }

        }

        private void closeDBConnection(bool stopFile)
        {
            if (stopFile)
            {
                Console.WriteLine("Finishing Application - 'Stop' File was found...");
            }
            else
            {
                Console.WriteLine("Finishing Application - Something called 'exit()'...");

            }
            if (simulateDB)
            {
                try
                {
                    dbOutput.Close();
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    Console.WriteLine(e.StackTrace);
                }
            }
        }

        private List<String> performQuery(int qLen)
        {
            List<String> zx = new List<String>();
            String s;
            int nbrToRead = requestsPerSecond - qLen;
            if (nbrToRead < minNbrToRead)
            {
                nbrToRead = minNbrToRead; // keep ticking over
            }
            for (int i = 0; i < nbrToRead; i++)
            {
                try
                {
                    s = dbOutput.ReadLine();
                    if (s == null)
                    {
                        stopping = true;
                        return zx;
                    }
                    zx.Add(s);
                }
                catch (IOException e)
                {
                    // TODO Auto-generated catch block
                    Console.WriteLine(e.StackTrace);
                }
            }
            if (zx.Count < nbrToRead)
            {
                stopping = true;
            }
            return zx;

        }
	
        
        
	    private bool allWorkersIdle() {
            lock (workerQueue) {
                if (workerQueue.Count != 0) {
                    return false;
                }
                for (int i=0;i<workers.Count;i++) {
                    if (workers[i].isProcessing) {
                        return false;
                    }
                }
                return true;
            }    
	    }

        
        public int getWorkerQLength() {
            lock (workerQueue) {
                return workerQueue.Count;
            }
        }
        public int getResultQLength() {
            lock (resultQueue) {
                return resultQueue.Count;
            }
        }
    
        public ServiceResult getFromResultQ() {
            lock (resultQueue) {
                ServiceResult sR = null;
                if (resultQueue.Count > 0)
                {
                    bool gotOne = resultQueue.TryDequeue(out sR);
                }
                return sR;
            }
        }

        public void putToWorkerQ(InputDTO dto) {
            lock (workerQueue) {
                workerQueue.Enqueue(dto);
            }
        }
    
        private void processResultQItem() {
            // process 1 entry in our input q
            Console.WriteLine("processing ResultQ len=" + getResultQLength());
            ServiceResult sr = getFromResultQ();
            if (sr != null)  // do stuff with it
            {
                //Console.WriteLine("res " + sr.getResult() + ", rec=" + sr.getDto().RowNbr + ", data [" + sr.getDto().Data + "]");
            }
            Thread.Sleep(pseudoProcessingSleepTimeValue);
        }
        public MultiThreadOrchestrator()
        {
        }
        public bool StopFileExists(String stFileName) {
            return System.IO.File.Exists( stFileName ); 
        }


   
    }
}
