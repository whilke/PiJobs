using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Fabric;
using System.Fabric.Description;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpticsService
{
    class OpticsSettings
    {
        #region Setting resolvers
        private static Func<ConfigurationPackage, ConfigurationSection> OpticsConfigPackage 
            = new Func<ConfigurationPackage,ConfigurationSection>((p) => 
            p.Settings.Sections["OpticsConfig"]);

        private static Func<ConfigurationSection, string, ConfigurationProperty>
            OpticsConfigProperty = new
            Func<ConfigurationSection, string, ConfigurationProperty>((s, k) =>
           s.Parameters[k]);

        private static Func<ConfigurationProperty, string> OpticsConfigPropertyToString
            = new Func<ConfigurationProperty, string>((p) =>
                (p != null && p.IsEncrypted) ? p.DecryptValue().ToString() : p?.Value
            );

        private Func<ConfigurationPackage, string, string> OpticsGetSetting
            = new Func<ConfigurationPackage, string, string>((p, k) => 
            OpticsConfigPropertyToString(OpticsConfigProperty(OpticsConfigPackage(p), k)));
        #endregion

        private ConfigurationPackage _package;
        public string Grain => OpticsGetSetting(_package, "Grain");
        public int QuerySize => int.Parse(OpticsGetSetting(_package, "QuerySize"));
        public OpticsSettings(ConfigurationPackage package)
        {
            _package = package;
        }
    }
}
