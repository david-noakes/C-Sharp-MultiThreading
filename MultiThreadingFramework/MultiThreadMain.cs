using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

/*
 * this is a structure for multi threading with a controller and many worker threads
 * Points to note:
 * 1. All database access is done in the controller (MultiThreadOrchestrator) to avoid
 *    potential deadlocks
 * 2. the controller has a rate limit, and reads enough rows to process in 1 second
 * 3. controller passes read data into the shared workerQueue for processing by workers
 *    this is a single queue multiple servers model to provide built in load levelling
 * 4. Each worker (MultiThreadWorker) takes an item from the queue, processes it
 *    and puts the result into the shared resultQueue for the controller
 * 5. Queue access is synchronized because although queues are thread safe, calling different methods
 *    in different threads uses different locks, and unsynchronized access can leave the queue in an
 *    inconsistent state
 * 6. the orchestrator will add additional workers if it is unable to reach the required processing rate
 *    subject to a maxTrheads limit
 * 7. if the processing loop has all workers idle in under 800 milliseconds, the orchestrator will
 *    remove 1 worker (subject to a minThreads limit)   
 * 8. the loop can be stopped by the presence of a stop file, or endofdata
 * 9. stopping is an orderly shutdown, where queues are processed until empty
 * 10. if orderly shutdown cannot be achieved in 5 seconds, the controller will abort each worker.         
 */

namespace MultiThreadingFramework
{
    class MultiThreadMain
    {
        static void Main(string[] args)
        {
            MultiThreadOrchestrator orchestrator = new MultiThreadOrchestrator();
            orchestrator.Orchestrate();
            Console.WriteLine("Processing Finished");
        }

    }
}
