using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Core.Blob;
using VSI.CloudPlatform.Core.Common;
using VSI.CloudPlatform.Core.Functions;
using VSI.CloudPlatform.Core.Telemetry;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Jobs;

namespace FunctionApp.AX365.ProductPrice
{
    public static partial class FUNC_DAX365Price
    {
        private static readonly string instrumentationKey = Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
        private static readonly bool _excludeDependency = FunctionUtilities.GetBoolValue(Environment.GetEnvironmentVariable("ExcludeDependency"), false);
        private static IBlob blob;
        private static ICloudDb cloudDb;

        static FUNC_DAX365Price()
        {
            blob = new AzureBlob(Environment.GetEnvironmentVariable("StorageConnectionString"));
            cloudDb = new CosmosCloudDb(Environment.GetEnvironmentVariable("CosmosConnectionString"));

            BindingRedirectApplicationHelper.Startup();
        }

        [FunctionName("FUNC_DAXProductPrice")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "POST", Route = null)] HttpRequestMessage req)
        {
            IOperationHolder<RequestTelemetry> operation = null;
            TelemetryClient telemetryClient = null;

            try
            {
                var request = await req.Content.ReadAsStringAsync();
                var functionParams = JsonConvert.DeserializeObject<FunctionParams>(request);

                var instanceKey = $"{functionParams.TransactionName}_{functionParams.PartnerShipId}_{functionParams.TransactionStep}";

                telemetryClient = TelemetryFactory.GetInstance(instanceKey, instrumentationKey, _excludeDependency);
                operation = telemetryClient.StartOperation<RequestTelemetry>(RCKFunctionNames.FUNC_RCK_D365_PRICE, Guid.NewGuid().ToString());

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Starting...");

                if (functionParams.partnerShip == null && !string.IsNullOrEmpty(functionParams.PartnerShipId))
                {
                    functionParams.partnerShip = CommonUtility.GetPartnerShip(cloudDb, functionParams.PartnerShipId);
                }

                if (functionParams.partnerShip == null)
                {
                    throw new Exception("Partnership object is empty!");
                }

                telemetryClient.AddDefaultProperties(functionParams);
               
                var requestParams = FunctionHelper.GetPriceRequestParams(functionParams);

                await FunctionHelper.ProcessAsync(requestParams, telemetryClient, cloudDb, blob);

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Finished.");

                return req.CreateResponse(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                var depthException = CommonUtility.GetDepthInnerException(ex);

                if (telemetryClient != null)
                {
                    telemetryClient.TrackException(depthException);
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "error occured" + ex);
                }

                throw;
            }
            finally
            {
                telemetryClient.StopOperation(operation);
            }
        }
    }
}