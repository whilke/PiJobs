using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Jobs
{
    public enum JobState
    {
        INVALID=0,
        CREATED,
        QUEUED,
        RUNNING,
        FINISHED
    }

    public class JobRecord
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public DateTime Created { get; set; }
        public JobState JobState { get; set; }
        public DataSession Session { get; set; }
        public string Data { get; set; }
        public int Cost { get; set; }
        public JobRecord()
        {
            Cost = 1; //hard coded for demo.
        }

        public JobRecord(DataSession session) : this()
        {
            Created = DateTime.UtcNow;
            Session = session;
            JobState = JobState.CREATED;
        }
    }
}
