using FunctionApp.RCK.D365.Price;
using Microsoft.ApplicationInsights;
using Newtonsoft.Json;
using RCK.CloudPlatform.AXD365;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model.Price;
using RCK.CloudPlatform.Model.Product;
using RCK.CloudPlatform.Model.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Db;
using VSI.CloudPlatform.Model.Common;
using VSI.CloudPlatform.Model.Jobs;

namespace FunctionApp.AX365.ProductPrice
{
    public class FunctionHelper
    {
        internal static PriceParams GetPriceRequestParams(FunctionParams functionParams)
        {
            var functionSettings = functionParams.Settings;
            var securityParams = JsonConvert.DeserializeObject<HttpSecurity>(functionSettings.GetValue("D365SecuritySettings"));

            return new PriceParams
            {
                D365RetailConnectivity = new D365RetailConnectivity
                {
                    AzAD = securityParams.Settings.GetValue("ADTenant"),
                    ClientId = securityParams.Settings.GetValue("ADClientAppId"),
                    ClientSeceret = securityParams.Settings.GetValue("ADClientAppSecret"),
                    D365Uri = securityParams.Settings.GetValue("ADResource"),
                    RetailServerUri = securityParams.Settings.GetValue("RCSUResource"),
                    OperatingUnitNumber = securityParams.Settings.GetValue("OUN")
                },
                ServicebusConnectionString = functionSettings.GetValue("ServicebusConnectionString"),
                MagentoStoreWebsite = functionSettings.GetValue("MagentoStoreWebsite"),
                MagentoStoreViewCode = functionSettings.GetValue("MagentoStoreViewCode"),
                CategoryLookupBlobUri = functionSettings.GetValue("CategoryLookupBlobUri"),
                ProductLookupBlobUri = functionSettings.GetValue("ProductLookupBlobUri"),
                PriceType = functionSettings.GetValue("PriceType"),
                MaxDegreeOfParallelism = Convert.ToInt32(Environment.GetEnvironmentVariable("MaxDegreeOfParallelism")),
                BatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("BatchSize")),
                FromDate = DateTime.Now.AddDays(1).Date,
                ToDate = DateTime.Now.AddDays(3).Date.AddSeconds(-1),
                TopicName = functionSettings.GetValue("TargetTopic"),
                ArchiveBlobContainer = Environment.GetEnvironmentVariable("ArchiveBlobContainer"),
                ProcessName = functionParams.TransactionStep,
                WorkfowName = functionParams.TransactionName,
                FunctionParams = functionParams
            };
        }

        internal async static Task ProcessAsync(PriceParams requestParams, TelemetryClient telemetryClient, ICloudDb cloudDb, IBlob blob)
        {
            var startDate = DateTime.Now;

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Connecting to Retail Server...");

            var retailContext = await RetailConnectivityController.ConnectToRetailServerAsync(requestParams.D365RetailConnectivity);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Connected with Retail Server.");

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Fetching products from blob storage...");

            var products = await GetProductAsync(retailContext);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Fetching base prices of store {requestParams.D365RetailConnectivity.OperatingUnitNumber}...");

            var productsWithBasePrices = PriceController.FetchBasePrices(requestParams, retailContext, products);

            if (productsWithBasePrices.Count > 0)
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Fetching categories from blob...");

                if (requestParams.PriceType.ToLower() == "base")
                {
                    var basePriceFileName = Environment.GetEnvironmentVariable("BasePriceFileName");
                    await TransmitBasePriceAsync(productsWithBasePrices, requestParams, telemetryClient, startDate, blob, cloudDb, basePriceFileName);
                }
                else
                {
                    var productCategories = await GetCategoriesAsync(retailContext);

                    if (requestParams.PriceType.ToLower() == "special")
                    {
                        var specialPriceFileName = Environment.GetEnvironmentVariable("SpecialPriceFileName");
                        await SpecialPriceHelper.TransmitSpecialPricesAsync(productCategories, productsWithBasePrices, requestParams, telemetryClient, startDate, blob, cloudDb, retailContext, requestParams.D365RetailConnectivity.OperatingUnitNumber, specialPriceFileName);
                    }

                    if (requestParams.PriceType.ToLower() == "tier")
                    {
                        var tierPriceFileName = Environment.GetEnvironmentVariable("TierPriceFileName");
                        await TierPriceHelper.TransmitTierPricesAsync(productCategories, productsWithBasePrices, requestParams, telemetryClient, startDate, blob, cloudDb, retailContext, requestParams.D365RetailConnectivity.OperatingUnitNumber, tierPriceFileName);
                    }

                    if (requestParams.PriceType.ToLower() == "deal")
                    {
                        var dealPriceFileName = Environment.GetEnvironmentVariable("DealPriceFileName");
                        await DealPriceHelper.TransmitDealPricesAsync(productCategories, productsWithBasePrices, requestParams, telemetryClient, startDate, blob, cloudDb, retailContext, requestParams.D365RetailConnectivity.OperatingUnitNumber, dealPriceFileName);
                    }
                }
            }
        }

        private async static Task TransmitBasePriceAsync(List<ProductPriceItem> productPriceItems, PriceParams priceParams, TelemetryClient telemetryClient, DateTime startDate, IBlob blob, ICloudDb cloudDb, string fileName)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Transmitting base price file...");

            var listOfBasePrices = productPriceItems.Select(pp => new ProductBasePrice
            {
                Sku = pp.Sku,
                BasePrice = pp.BasePrice,
                StoreViewCode = priceParams.MagentoStoreViewCode,
                UOM = pp.UOM
            }).ToList();

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total base prices {listOfBasePrices.Count}...");

            var prices = listOfBasePrices.GroupBy(p => new { p.Sku, p.BasePrice }).Select(p => new ProductBasePrice
            {
                BasePrice = p.Key.BasePrice,
                StoreViewCode = p.FirstOrDefault().StoreViewCode,
                Sku = p.Key.Sku,
                UOM = p.FirstOrDefault().UOM
            });

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total base prices after grouping by SKU, Price {listOfBasePrices.Count}...");

            var priceWrapper = new PriceWrapper<ProductBasePrice> { Prices = prices.ToList() };

            if (prices.Count() > 0)
            {
                var stage = CommonUtility.CreateStageMessage(priceParams.FunctionParams, priceParams.FunctionParams.partnerShip.Transaction_Direction, fileName);

                try
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploading Original message to blob storage.");

                    var bloblUrl = await CommonUtility.UploadOutboundDataFilesOnBlobAsync(priceParams.FunctionParams, blob, JsonConvert.SerializeObject(priceWrapper), stage.Transaction_Id, priceParams.ArchiveBlobContainer);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploaded Original message to blob storage.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI Success Message.");

                    stage.Data = bloblUrl;
                    stage.EDI_STANDARD_DOCUMENT = bloblUrl;
                    CommonUtility.LogSuccessStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, OverallStatus.IN_PROGRESS, MessageStatus.COMPLETED, bloblUrl, stepName: fileName);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI Success Message.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI message in Topic.");

                    var messageProperties = new Dictionary<string, string> { { "FILENAME", $"{fileName}.json" } };
                    await CommonUtility.SendMessageToTopicAsync(JsonConvert.SerializeObject(stage), priceParams.ServicebusConnectionString, priceParams.TopicName, messageProperties);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI message in Topic.");

                    //var blobContent = JsonConvert.SerializeObject(productPriceItems);
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"BasePrice/Yesterday/{storeId}.json", priceParams.BlobDirectory).Wait();
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"BasePrice/Delta/{storeId}.json", priceParams.BlobDirectory).Wait();
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);

                    CommonUtility.LogErrorStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, depthException, true);

                    telemetryClient.TrackException(depthException);
                }
            }
        }

        private static async Task<List<ProductBlob>> GetProductAsync(D365RetailServerContext retailServerContext)
        {
         
            var productIds = await ProductController.GetProductIdsAsync(retailServerContext);
            
         return productIds;
        }

        private static async Task<List<ProductCategoryBlob>> GetCategoriesAsync(D365RetailServerContext retailServerContext)
        {
            var categories = await CategoryController.GetChannelCategoriesAsync(retailServerContext);

            return categories;
        }
    }
}