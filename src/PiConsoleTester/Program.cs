using PiJobs.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiConsoleTester
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch sw = Stopwatch.StartNew();
            var ds = new DataSession("111111","000001","000000");
            ServiceResolver.Router.AddOrGet(ds).GetAwaiter().GetResult();
            Console.WriteLine("Created in " + sw.Elapsed.ToString());
            sw = Stopwatch.StartNew();

            ds.PiQueue().AddTask(ds, "100").GetAwaiter().GetResult();
            while(true)
            {
                var r = ds.PiQueue().GetStatus(ds).GetAwaiter().GetResult();
                if (r != PiJobs.Shared.Jobs.JobState.FINISHED)
                {
                    Task.Delay(100).GetAwaiter().GetResult();
                }
                else
                {
                    ds.PiQueue().RemoveStatus(ds).GetAwaiter().GetResult();
                    break;
                }
            }
            Console.WriteLine("Executed in " + sw.Elapsed.ToString());
            sw = Stopwatch.StartNew();

            var data = ds.PiDataSession().FetchData().GetAwaiter().GetResult();
            Console.WriteLine(data);
            ServiceResolver.Router.Close(ds).GetAwaiter().GetResult();

            Console.WriteLine("Closed in " + sw.Elapsed.ToString());
            sw = Stopwatch.StartNew();


        }
    }
}
