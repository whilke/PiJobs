using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared
{
    public class DataSession : IEquatable<DataSession>, IComparable<DataSession>
    {
        public string Id { get; set; }
        public string Account { get; set; }
        public string DataId { get; set; }
        public string UserId { get; set; }

        public DataSession()
        {

        }

        public DataSession(string account, string dataId, string userId): this()
        {
            Account = account;
            DataId = dataId;
            UserId = userId;
            Id = Account + "-" + UserId + "-" + DataId;
        }

        public bool Equals(DataSession other)
        {
            if (other == null) return false;
            return other.Id == Id;
        }

        public override bool Equals(object obj)
        {
            return Equals((DataSession)obj);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }

        public int CompareTo(DataSession other)
        {
            return Id.CompareTo(other.Id);
        }
    }
}
