using PiJobs.Shared;
using PiJobs.Shared.Jobs;
using PiJobs.Shared.Optics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace PiJobs.API.Controllers
{
    public class JobRun
    {
        public DateTime dtStart;
        public float queueTime;
        public float execTime;
    }

    public class AccountInfo
    {
        public int CUSize { get; set; }
        public int QueueSize { get; set; }
        public int RunningSize { get; set; }
        public int TotalJobs { get; set; }
        public float AvgExecTime { get; set; }
        public float AvgQueueTime { get; set; }
    }

    [RoutePrefix("admin/api")]
    public class AdminController : ApiController
    {
        [HttpGet]
        [Route("accountQueues")]
        public async Task<Dictionary<string, AccountInfo>> Get()
        {
            Dictionary<string, AccountInfo> data = new Dictionary<string, AccountInfo>();
            var accounts = await ServiceResolver.Router.GetAccountList();
            foreach(var account in accounts)
            {
                var ds = new DataSession(account, "", "");
                var jobs = await GetJobs(account);
                var stats = await ds.PiQueue().GetStats();

                var info = new AccountInfo();
                info.CUSize = stats.Item1;
                info.TotalJobs = jobs.Count();
                info.QueueSize = stats.Item2;
                info.RunningSize = stats.Item3;
                if (info.TotalJobs > 0)
                {
                    info.AvgQueueTime = jobs.Where(e=>e.queueTime>=0).DefaultIfEmpty().Average(e => e==null? 0: e.queueTime);
                    info.AvgExecTime = jobs.Where(e => e.execTime >= 0).DefaultIfEmpty().Average(e => e == null ? 0 : e.execTime);
                }

                data[account] = info;
            }
            return data;
        }

        private async Task<List<JobRun>> GetJobs(string account)
        {

            List<JobRun> runs = new List<JobRun>();
            var client = new OpticsClient();
            var results = await client.Query(
                "TYPE = \"JOB\" and ACCOUNT = \""+account+"\"", 
                DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow);

            //sort by jobId
            var jobs = results.GroupBy(e => e.Properties["SID"]);
            foreach(var job in jobs)
            {
                var jId = job.Key;
                var queueStart = job.FirstOrDefault(e => e.Properties["EVENT"] == "QUEUE" && e.Properties["STATE"] == "START");
                var queueEnd = job.FirstOrDefault(e => e.Properties["EVENT"] == "QUEUE" && e.Properties["STATE"] == "FINISH");
                var execStart = job.FirstOrDefault(e => e.Properties["EVENT"] == "COMPUTE" && e.Properties["STATE"] == "START");
                var execEnd = job.FirstOrDefault(e => e.Properties["EVENT"] == "COMPUTE" && e.Properties["STATE"] == "FINISH");

                if (queueStart == null)
                    continue;

                JobRun run = new JobRun();
                run.dtStart = queueStart.Timestamp;
                if (queueEnd == null)
                {
                    run.queueTime = -1;
                }
                else
                {
                    run.queueTime = (float)(queueEnd.Timestamp - queueStart.Timestamp).TotalMilliseconds;
                }

                if (execEnd == null )
                {
                    run.execTime = -1;
                }
                else
                {
                    run.execTime = (float)(execEnd.Timestamp - execStart.Timestamp).TotalMilliseconds;
                }
                runs.Add(run);
            }

            return runs;
        }

        #region Load Test
        [HttpGet]
        [Route("loadTest")]
        public Task LoadTest()
        {
            //start and run a load test.
            var t = Task.Run(() =>
            {
                var t1 = RunAccountTest("000001", 6);
                var t2 = RunAccountTest("000002", 10);
                //var t3 = RunAccountTest("000003", 12);
            });
            return Task.FromResult(0);
        }

        private async Task RunAccountTest(string account, int count)
        {
            List<Task> tasks = new List<Task>();
            for(int i=0; i<count;++i)
            {
                var id = "0000" + i;
                var ds = new DataSession(account, id, id);
                tasks.Add(RunUserTest(ds));
            }
            await Task.WhenAll(tasks.ToArray());
        }

        private async Task RunUserTest(DataSession session)
        {
            try
            {
                var r = new Random();
                var sizeList = new string[]{"1000","5000"};
                await Task.Delay(r.Next(1, 5) * 1000);
                await ServiceResolver.Router.AddOrGet(session);
                for (int i = 0; i < 5000; ++i)
                {
                    //delay random time and start new test.
                    var randomWait = r.Next(1, 5) * 1000;
                    await Task.Delay(randomWait);

                    var sizeIndex = r.Next(sizeList.Length);
                    try
                    {
                        await session.PiQueue().AddTask(session, sizeList[sizeIndex]);
                    }
                    catch
                    {
                        continue;
                    }

                    while (true)
                    {
                        var s = await session.PiQueue().GetStatus(session);
                        if (s == JobState.FINISHED)
                        {
                            await session.PiQueue().RemoveStatus(session);
                            break;
                        }
                        await Task.Delay(1000);
                    }

                }
            }
            catch
            {
            }
            finally
            {
                await ServiceResolver.Router.Close(session);
            }

        }
        #endregion
    }
}
