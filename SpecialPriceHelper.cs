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
    public class SpecialPriceHelper
    {
        public async static Task TransmitSpecialPricesAsync(List<ProductCategoryBlob> productCategories, List<ProductPriceItem> productsWithBasePrices, PriceParams priceParams, TelemetryClient telemetryClient,
            DateTime startDate, IBlob blob, ICloudDb cloudDb, D365RetailServerContext retailContext, string channel, string fileName)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Fetching simple discounts from D365...");

            var simpleDiscounts = PriceController.FetchSimpleDiscounts(channel, retailContext);

            if (simpleDiscounts == null || simpleDiscounts.Count == 0)
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"No simple discounts found in D365 for store {channel}.");

                return;
            }

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {simpleDiscounts.Count} simple discounts found in D365 for store {channel}.");

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Getting products special prices...");

            var specialPrices = GetProductsSpecialPrices(simpleDiscounts, productsWithBasePrices, productCategories, priceParams, channel);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {specialPrices.Count} special prices found.");

            if (specialPrices.Count() > 0)
            {
                var priceWrapper = new PriceWrapper<ProductSpecialPrice> { Prices = specialPrices };

                var stage = CommonUtility.CreateStageMessage(priceParams.FunctionParams, priceParams.FunctionParams.partnerShip.Transaction_Direction, fileName);

                try
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploading Original message (special prices) to blob storage...");

                    var bloblUrl = await CommonUtility.UploadOutboundDataFilesOnBlobAsync(priceParams.FunctionParams, blob, JsonConvert.SerializeObject(priceWrapper), stage.Transaction_Id, priceParams.ArchiveBlobContainer);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploaded Original message (special prices) to blob storage.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI Success Message (special prices)...");

                    stage.Data = bloblUrl;
                    stage.EDI_STANDARD_DOCUMENT = bloblUrl;
                    CommonUtility.LogSuccessStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, OverallStatus.IN_PROGRESS, MessageStatus.COMPLETED, bloblUrl, stepName: fileName);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI Success Message (special prices).");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI message (special prices) in Topic...");

                    var messageProperties = new Dictionary<string, string> { { "FILENAME", $"{fileName}.json" } };
                    await CommonUtility.SendMessageToTopicAsync(JsonConvert.SerializeObject(stage), priceParams.ServicebusConnectionString, priceParams.TopicName, messageProperties);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI message (special prices) in Topic.");

                    //var blobContent = JsonConvert.SerializeObject(productSpecialPrices);
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"SimpleDiscountPrices/Yesterday/{storeId}.json", priceParams.BlobDirectory).Wait();
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"SimpleDiscountPrices/Delta/{storeId}.json", priceParams.BlobDirectory).Wait();
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);

                    CommonUtility.LogErrorStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, depthException, true);

                    telemetryClient.TrackException(depthException);
                }
            }
        }

        private static List<ProductSpecialPrice> GetProductsSpecialPrices(List<SimpleDiscount> simpleDiscounts, List<ProductPriceItem> productsList, List<ProductCategoryBlob> blobCategories, PriceParams priceParams, string channel)
        {
            var offers = simpleDiscounts.GroupBy(key => key.OfferId);
            var specialPriceList = new List<ProductSpecialPrice>();

            foreach (var offer in offers)
            {
                foreach (var discount in offer)
                {
                    if (discount.DiscountMethod == "0" && discount.Discount == 0)
                    {
                        continue;
                    }

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
                            var specialPrice = new ProductSpecialPrice()
                            {
                                Sku = productPriceItem.Sku,
                                SpecialPrice = GetDiscountPrice(productPriceItem.BasePrice, discount),
                                SpecialPriceFeed = GetDiscountPrice(productPriceItem.BasePrice, discount),
                                EligibleForPromo = "no",
                                ValidFrom = discount.ValidFrom.DateTime.ToShortDateString(),
                                ValidTo = discount.ValidTo.DateTime.ToShortDateString(),
                                StoreViewCode = priceParams.MagentoStoreViewCode
                            };

                            specialPriceList.Add(specialPrice);
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
                                var specialPrice = new ProductSpecialPrice()
                                {
                                    Sku = product.Sku,
                                    SpecialPrice = GetDiscountPrice(product.BasePrice, discount),
                                    SpecialPriceFeed = GetDiscountPrice(product.BasePrice, discount),
                                    EligibleForPromo = "no",
                                    ValidFrom = discount.ValidFrom.DateTime.ToShortDateString(),
                                    ValidTo = discount.ValidTo.DateTime.ToShortDateString(),
                                    StoreViewCode = priceParams.MagentoStoreViewCode
                                };

                                specialPriceList.Add(specialPrice);
                            }

                        }

                        foreach (var subCategory in blobCategories.Where(filter => filter.ParentCategory == category.RecordId))
                        {
                            if (!excludedCategories.Contains(subCategory.RecordId))
                            {
                                specialPriceList.AddRange(GetSubCategorySimpleDiscounts(discount, productsList, channel, subCategory.RecordId, excludedCategories, excludedProducts, excludedVariants, priceParams, blobCategories));
                            }
                        }
                    }

                }
            }

            return specialPriceList;
        }

        private static List<ProductSpecialPrice> GetSubCategorySimpleDiscounts(SimpleDiscount discount, List<ProductPriceItem> productsList, string storeId, long categoryId,
            List<long> exclusionList, List<long> excludedProducts, List<long> excludedVariants, PriceParams priceParams, List<ProductCategoryBlob> blobCategories)
        {
            var specialPriceList = new List<ProductSpecialPrice>();

            var products = productsList.Where(filter => filter.Category == categoryId);

            foreach (var product in products)
            {
                if (excludedProducts.Contains(product.RecordId) || excludedVariants.Contains(product.RecordId))
                {
                    continue;
                }

                if (discount.UOM == "" || discount.UOM == product.UOM)
                {
                    var specialPrice = new ProductSpecialPrice()
                    {
                        Sku = product.Sku,
                        SpecialPrice = GetDiscountPrice(product.BasePrice, discount),
                        SpecialPriceFeed = GetDiscountPrice(product.BasePrice, discount),
                        EligibleForPromo = "no",
                        ValidFrom = discount.ValidFrom.DateTime.ToShortDateString(),
                        ValidTo = discount.ValidTo.DateTime.ToShortDateString(),
                        StoreViewCode = priceParams.MagentoStoreViewCode
                    };

                    specialPriceList.Add(specialPrice);
                }
            }

            foreach (var subCategory in blobCategories.Where(filter => filter.ParentCategory == categoryId))
            {
                if (!exclusionList.Contains(subCategory.RecordId))
                {
                    specialPriceList.AddRange(GetSubCategorySimpleDiscounts(discount, productsList, storeId, subCategory.RecordId, exclusionList, excludedProducts, excludedVariants, priceParams, blobCategories));
                }
            }

            return specialPriceList;
        }

        private static decimal GetDiscountPrice(decimal basePrice, SimpleDiscount simpleDiscount)
        {
            var discount = simpleDiscount.OfferPrice;

            if (simpleDiscount.Discount > 0)
            {
                discount = basePrice - (simpleDiscount.Discount / 100) * basePrice;
            }
            else if (simpleDiscount.DiscountAmount > 0)
            {
                discount = basePrice - simpleDiscount.DiscountAmount;
            }

            return discount;
        }
    }
}