using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MultiThreadingFramework
{
    class CommonConstants
    {
 	    public static String statusProcessing = "PROCESSING";
	    public static String statusFailed = "FAILED";
	    public static String statusRetry = "RETRY";
	    public static String statusSuccess = "SUCCESS"; 

        public static long currentTimeMillis()
        {
            long milliseconds = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            return milliseconds;
        }
        public static long currentTimeMicros()  // there appear to be 10,000 ticks per millisecond
        {                                       // as long as there are more than 1000, this code will work
            long microseconds = DateTime.Now.Ticks / (TimeSpan.TicksPerMillisecond / 1000);
            return microseconds;
        }

        public static String currentTimeExtended ()
        {
            String longish = DateTime.Now.Ticks.ToString();
            String blah = DateTime.Now.ToString("HH:mm:ss:fff");
            return blah;
        }
    }
}
