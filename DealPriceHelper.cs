using Microsoft.ApplicationInsights;
using Newtonsoft.Json;
using RCK.CloudPlatform.AXD365;
using RCK.CloudPlatform.Common.Constants;
using RCK.CloudPlatform.Common.Utilities;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model.Price;
using RCK.CloudPlatform.Model.Product;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Common.Interfaces;
using VSI.CloudPlatform.Db;

namespace FunctionApp.RCK.D365.Price
{
    public class DealPriceHelper
    {
        public async static Task TransmitDealPricesAsync(List<ProductCategoryBlob> productCategories, List<ProductPriceItem> productsWithBasePrices, PriceParams priceParams, TelemetryClient telemetryClient,
        DateTime startDate, IBlob blob, ICloudDb cloudDb, D365RetailServerContext retailContext, string channel, string fileName)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Fetching deal price discounts from D365 for store {channel}...");

            var dealDiscounts = PriceController.FetchMixAndMatchDealPrices(channel, retailContext);

            if (dealDiscounts == null || dealDiscounts.Count == 0)
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"No deal discounts found for store {channel}.");

                return;
            }

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {dealDiscounts.Count} deal discounts found in D365 for store {channel}.");

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Getting products deal prices...");

            var dealPrices = GetProductsDealPrices(dealDiscounts, productsWithBasePrices, productCategories, priceParams, channel);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {dealPrices.Count} deal prices found.");

            if (dealPrices.Count() > 0)
            {
                var combinedDealPrices = new List<ProductDealPrice>();

                var offerGroups = dealPrices.GroupBy(dp => dp.OfferId);

                foreach (var offerGroup in offerGroups)
                {
                    combinedDealPrices.Add(new ProductDealPrice
                    {
                        OfferId = offerGroup.FirstOrDefault().OfferId,
                        Skus = string.Join(",", offerGroup.Select(dp => dp.Skus).ToArray()),
                        Discount = (offerGroup.Sum(dp => dp.BasePrice) - offerGroup.FirstOrDefault().DealPrice),
                        DealPrice = offerGroup.FirstOrDefault().DealPrice,
                        Name = offerGroup.FirstOrDefault().Name,
                        Description = offerGroup.FirstOrDefault().Description,
                        Status = offerGroup.FirstOrDefault().Status,
                        Website = offerGroup.FirstOrDefault().Website
                    });
                }

                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {combinedDealPrices.Count} deal prices found after grouping.");

                if (combinedDealPrices.Count == 0)
                {
                    return;
                }

                var priceWrapper = new PriceWrapper<ProductDealPrice> { Prices = combinedDealPrices };

                var stage = CommonUtility.CreateStageMessage(priceParams.FunctionParams, priceParams.FunctionParams.partnerShip.Transaction_Direction, fileName);

                try
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploading Original message (deal prices) to blob storage...");

                    var bloblUrl = await CommonUtility.UploadOutboundDataFilesOnBlobAsync(priceParams.FunctionParams, blob, JsonConvert.SerializeObject(priceWrapper), stage.Transaction_Id, priceParams.ArchiveBlobContainer);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploaded Original message (deal prices) to blob storage.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI Success Message (deal prices)...");

                    stage.Data = bloblUrl;
                    stage.EDI_STANDARD_DOCUMENT = bloblUrl;
                    CommonUtility.LogSuccessStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, OverallStatus.IN_PROGRESS, MessageStatus.COMPLETED, bloblUrl, stepName: fileName);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI Success Message (deal prices).");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI message (deal prices) in Topic...");

                    var messageProperties = new Dictionary<string, string> { { "FILENAME", $"{fileName}.json" } };
                    await CommonUtility.SendMessageToTopicAsync(JsonConvert.SerializeObject(stage), priceParams.ServicebusConnectionString, priceParams.TopicName, messageProperties);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI message (deal prices) in Topic.");

                    //var blobContent = JsonConvert.SerializeObject(combinedDealPrices);
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"DealPrices/Yesterday/{storeId}.json", priceParams.BlobDirectory).Wait();
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"DealPrices/Delta/{storeId}.json", priceParams.BlobDirectory).Wait();
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);

                    CommonUtility.LogErrorStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, depthException, true);

                    telemetryClient.TrackException(depthException);
                }
            }
        }

        private static List<ProductDealPrice> GetProductsDealPrices(List<MixAndMatchDealPrice> dealDiscounts, List<ProductPriceItem> productsList, List<ProductCategoryBlob> blobCategories, PriceParams priceParams, string channel)
        {
            var offers = dealDiscounts.GroupBy(key => key.OfferId);
            var dealPriceList = new List<ProductDealPrice>();

            foreach (var offer in offers)
            {
                foreach (var discount in offer)
                {
                    ProductPriceItem productPriceItem = null;

                    if (discount.Product > 0 && discount.LineType == 0)
                    {
                        if (discount.Variant > 0)
                        {
                            productPriceItem = productsList.FirstOrDefault(filter => filter.RecordId == discount.Variant && (discount.UOM == "" || discount.UOM.ToLower() == filter.UOM.ToLower()));
                        }
                        else
                        {
                            productPriceItem = productsList.FirstOrDefault(filter => filter.RecordId == discount.Product && (discount.UOM == "" || discount.UOM.ToLower() == filter.UOM.ToLower()));
                        }

                        if (productPriceItem != null)
                        {
                            var dealPrice = new ProductDealPrice()
                            {
                                OfferId = offer.Key,
                                Skus = productPriceItem.Sku,
                                BasePrice = productPriceItem.BasePrice,
                                DealPrice = discount.OfferPrice,
                                Name = discount.Name,
                                Description = discount.Description,
                                Website = priceParams.MagentoStoreWebsite,
                                Status = 0
                            };

                            dealPriceList.Add(dealPrice);
                        }
                    }
                    else if (discount.Category > 0 && discount.Product == 0 && discount.LineType == 0)
                    {
                        var category = blobCategories.FirstOrDefault(filter => filter.RecordId == discount.Category);
                        var excludedCategories = offer.Where(filter => filter.Category > 0 && filter.Product == 0 && filter.LineType == 1).Select(key => key.Category).ToList();
                        var excludedProducts = offer.Where(filter => filter.Product > 0 && filter.Variant == 0 && filter.LineType == 1).Select(key => key.Product).ToList();
                        var excludedVariants = offer.Where(filter => filter.Product > 0 && filter.Variant > 0 && filter.LineType == 1).Select(key => key.Variant).ToList();

                        if (category == null)
                        {
                            continue;
                        }

                        var products = productsList.Where(filter => filter.Category == category.RecordId);

                        foreach (var product in products)
                        {
                            if (excludedProducts.Contains(product.RecordId) || excludedVariants.Contains(product.RecordId))
                            {
                                continue;
                            }

                            if (discount.UOM == "" || discount.UOM == product.UOM)
                            {
                                var dealPrice = new ProductDealPrice()
                                {
                                    OfferId = offer.Key,
                                    Skus = product.Sku,
                                    BasePrice = product.BasePrice,
                                    DealPrice = discount.OfferPrice,
                                    Name = discount.Name,
                                    Description = discount.Description,
                                    Website = priceParams.MagentoStoreWebsite,
                                    Status = 0
                                };

                                dealPriceList.Add(dealPrice);
                            }

                        }

                        foreach (var subCategory in blobCategories.Where(filter => filter.ParentCategory == category.RecordId))
                        {
                            if (!excludedCategories.Contains(subCategory.RecordId))
                            {
                                dealPriceList.AddRange(GetSubCategoryDealPrices(discount, productsList, channel, subCategory.RecordId, excludedCategories, excludedProducts, excludedVariants, priceParams, blobCategories));
                            }
                        }
                    }

                }
            }

            return dealPriceList;
        }

        private static List<ProductDealPrice> GetSubCategoryDealPrices(MixAndMatchDealPrice discount, List<ProductPriceItem> productsList, string storeId, long categoryId,
            List<long> exclusionList, List<long> excludedProducts, List<long> excludedVariants, PriceParams priceParams, List<ProductCategoryBlob> blobCategories)
        {
            var dealPriceList = new List<ProductDealPrice>();

            var products = productsList.Where(filter => filter.Category == categoryId);

            foreach (var product in products)
            {
                if (excludedProducts.Contains(product.RecordId) || excludedVariants.Contains(product.RecordId))
                {
                    continue;
                }

                if (discount.UOM == "" || discount.UOM == product.UOM)
                {
                    var dealPrice = new ProductDealPrice
                    {
                        OfferId = discount.OfferId,
                        Skus = product.Sku,
                        BasePrice = product.BasePrice,
                        DealPrice = discount.OfferPrice,
                        Name = discount.Name,
                        Description = discount.Description,
                        Website = priceParams.MagentoStoreWebsite,
                        Status = 0
                    };

                    dealPriceList.Add(dealPrice);
                }
            }

            foreach (var subCategory in blobCategories.Where(filter => filter.ParentCategory == categoryId))
            {
                if (!exclusionList.Contains(subCategory.RecordId))
                {
                    dealPriceList.AddRange(GetSubCategoryDealPrices(discount, productsList, storeId, subCategory.RecordId, exclusionList, excludedProducts, excludedVariants, priceParams, blobCategories));
                }
            }

            return dealPriceList;
        }
    }
}
