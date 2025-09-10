using System;
using System.Collections.Generic;
using System.Text;
using Altaworx.AWS.Core;

namespace AltaworxAWSGetOptimizationUsage
{
    public class JasperUsageReportResult
    {
        public int ServiceProviderId { get; set; }
        public int CarrierRatePlanId { get; set; }
        public string RatePlanName { get; set; }

        public bool IsLastPage { get; set; }
        public int LastPageNumberProcessed { get; set; }
        public int VersionNumber { get; set; }
        public int LastVersionProcessed { get; set; }
        public bool IsLastVersion { get => LastVersionProcessed <= 1; }
        public List<JasperUsageReportRecord> UsageRecords { get; set; }

        public JasperUsageReportResult(int serviceProviderId, int carrierRatePlanId, string ratePlanName)
        {
            ServiceProviderId = serviceProviderId;
            CarrierRatePlanId = carrierRatePlanId;
            RatePlanName = ratePlanName;
        }

        internal void AddUsageRecords(JasperGetUsageByRatePlanResponse response)
        {
            if (response.zones != null && response.zones.Count > 0)
            {
                foreach (var zone in response.zones)
                {
                    if (zone.devices != null && zone.devices.Count > 0)
                    {
                        foreach (var device in zone.devices)
                        {
                            UsageRecords.Add(new JasperUsageReportRecord() { DataUsage = device.usage.dataUsage, ICCID = device.deviceId });
                        }
                    }
                }
            }
        }
    }

    public class JasperUsageReportRecord
    {
        public string ICCID { get; set; }
        public long DataUsage { get; set; }
    }

    public class JasperGetUsageByRatePlanResponse
    {
        public string ratePlanName { get; set; }
        public List<JasperZone> zones { get; set; }
        public int pageNumber { get; set; }
        public bool lastPage { get; set; }
        public int ratePlanVersion { get; set; }
    }

    public class JasperZone
    {
        public string zone { get; set; }
        public List<JasperDeviceRecord> devices { get; set; }
    }

    public class JasperDeviceRecord
    {
        public string deviceId { get; set; }
        public JasperDeviceUsageRecord usage { get; set; }
    }

    public class JasperDeviceUsageRecord
    {
        public long dataUsage { get; set; }
    }
}
