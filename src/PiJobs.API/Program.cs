using System;
using System.Diagnostics;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.Owin.Hosting;

namespace PiJobs.API
{
    internal static class Program
    {
        public static class NetAclChecker
        {
            public static void AddAddress(string address)
            {
                AddAddress(address, Environment.UserDomainName, Environment.UserName);
            }

            public static void AddAddress(string address, string domain, string user)
            {
                string args = string.Format(@"http add urlacl url={0} user={1}\{2}", address, domain, user);

                ProcessStartInfo psi = new ProcessStartInfo("netsh", args);
                psi.Verb = "runas";
                psi.CreateNoWindow = true;
                psi.WindowStyle = ProcessWindowStyle.Hidden;
                psi.UseShellExecute = true;

                Process.Start(psi).WaitForExit();
            }
        }

        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main(string[] args)
        {

            try
            {
                if (args != null && args.Length > 0 && args[0] == "standalone")
                {

                    var listeningAddress = string.Format(
                        System.Globalization.CultureInfo.InvariantCulture,
                        "http://+:{0}/{1}",
                        4000, "");
                    NetAclChecker.AddAddress(listeningAddress);
                    var webApp = WebApp.Start(listeningAddress, Startup.ConfigureApp);
                    Console.ReadLine();
                }
            }
            catch
            {

            }

            try
            {
                // The ServiceManifest.XML file defines one or more service type names.
                // Registering a service maps a service type name to a .NET type.
                // When Service Fabric creates an instance of this service type,
                // an instance of the class is created in this host process.

                ServiceRuntime.RegisterServiceAsync("APIType",
                    context => new API(context)).GetAwaiter().GetResult();

                ServiceEventSource.Current.ServiceTypeRegistered(Process.GetCurrentProcess().Id, typeof(API).Name);

                // Prevents this host process from terminating so services keeps running. 
                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceHostInitializationFailed(e.ToString());
                throw;
            }
        }
    }
}
