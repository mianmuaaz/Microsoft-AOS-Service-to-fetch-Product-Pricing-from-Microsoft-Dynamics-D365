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
    public class TierPriceHelper
    {
        public async static Task TransmitTierPricesAsync(List<ProductCategoryBlob> productCategories, List<ProductPriceItem> productsWithBasePrices, PriceParams priceParams, TelemetryClient telemetryClient,
            DateTime startDate, IBlob blob, ICloudDb cloudDb, D365RetailServerContext retailContext, string channel, string fileName)
        {
            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Fetching quantity discounts from D365 for store {channel}...");

            var quantityDiscounts = PriceController.FetchQuantityDiscounts(channel, retailContext);

            if (quantityDiscounts == null || quantityDiscounts.Count == 0)
            {
                telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"No quantity discounts found for store {channel}.");

                return;
            }

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {quantityDiscounts.Count} quantity discounts found in D365 for store {channel}.");

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Getting products tier prices...");

            var tierPrices = GetProductsTierPrices(productCategories, productsWithBasePrices, quantityDiscounts, priceParams, channel);

            telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Total {tierPrices.Count} tier prices found.");

            if (tierPrices.Count() > 0)
            {
                var priceWrapper = new PriceWrapper<ProductTierPrice> { Prices = tierPrices };

                var stage = CommonUtility.CreateStageMessage(priceParams.FunctionParams, priceParams.FunctionParams.partnerShip.Transaction_Direction, fileName);

                try
                {
                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploading Original message (tier prices) to blob storage...");

                    var bloblUrl = await CommonUtility.UploadOutboundDataFilesOnBlobAsync(priceParams.FunctionParams, blob, JsonConvert.SerializeObject(priceWrapper), stage.Transaction_Id, priceParams.ArchiveBlobContainer);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, $"Uploaded Original message (tier prices) to blob storage.");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI Success Message (tier prices)...");

                    stage.Data = bloblUrl;
                    stage.EDI_STANDARD_DOCUMENT = bloblUrl;
                    CommonUtility.LogSuccessStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, OverallStatus.IN_PROGRESS, MessageStatus.COMPLETED, bloblUrl, stepName: fileName);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI Success Message (tier prices).");

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushing EDI message (tier prices) in Topic...");

                    var messageProperties = new Dictionary<string, string> { { "FILENAME", $"{fileName}.json" } };
                    await CommonUtility.SendMessageToTopicAsync(JsonConvert.SerializeObject(stage), priceParams.ServicebusConnectionString, priceParams.TopicName, messageProperties);

                    telemetryClient.TrackTrace(RCKFunctionNames.FUNC_RCK_D365_PRICE, "Pushed EDI message (tier prices) in Topic.");

                    //var blobContent = JsonConvert.SerializeObject(tierPrices);
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"QuantityDiscountPrices/Yesterday/{storeId}.json", priceParams.BlobDirectory).Wait();
                    //blob.UploadFileToBlobStorageAsync(blobContent, $"QuantityDiscountPrices/Delta/{storeId}.json", priceParams.BlobDirectory).Wait();
                }
                catch (Exception ex)
                {
                    var depthException = CommonUtility.GetDepthInnerException(ex);

                    CommonUtility.LogErrorStageMessage(cloudDb, stage, priceParams.FunctionParams, startDate, depthException, true);

                    telemetryClient.TrackException(depthException);
                }
            }
        }

        private static List<ProductTierPrice> GetProductsTierPrices(List<ProductCategoryBlob> productCategories, List<ProductPriceItem> productsWithBasePrices, List<QuantityDiscount> discounts, PriceParams priceParams, string storeId)
        {
            var offers = discounts.GroupBy(key => key.OfferId);
            var tierPriceList = new List<ProductTierPrice>();

            foreach (var offer in offers)
            {
                foreach (var discount in offer)
                {
                    ProductPriceItem productPriceItem = null;

                    if (discount.Product > 0 && discount.LineType == 0)
                    {
                        if (discount.Variant > 0)
                        {
                            productPriceItem = productsWithBasePrices.FirstOrDefault(filter => filter.RecordId == discount.Variant && (discount.UOM == "" || discount.UOM.ToLower() == filter.UOM.ToLower()));
                        }
                        else
                        {
                            productPriceItem = productsWithBasePrices.FirstOrDefault(filter => filter.RecordId == discount.Product && (discount.UOM == "" || discount.UOM.ToLower() == filter.UOM.ToLower()));
                        }

                        if (productPriceItem != null)
                        {
                            var tierPrice = new ProductTierPrice()
                            {
                                Sku = productPriceItem.Sku,
                                Quantity = Math.Round(discount.LowestQty, 2).ToString(),
                                TierPrice = GetQuantityDiscountPrice(productPriceItem.BasePrice, discount),
                                Website = priceParams.MagentoStoreWebsite,
                                CustomerGroup = "ALL GROUPS",
                                ValueType = "Fixed"
                            };

                            tierPriceList.Add(tierPrice);
                        }
                    }
                    else if (discount.Category > 0 && discount.Product == 0 && discount.LineType == 0)
                    {
                        var category = productCategories.FirstOrDefault(filter => filter.RecordId == discount.Category);
                        var excludedCategories = offer.Where(filter => filter.Category > 0 && filter.Product == 0 && filter.LineType == 1).Select(key => key.Category).ToList();
                        var excludedProducts = offer.Where(filter => filter.Product > 0 && filter.Variant == 0 && filter.LineType == 1).Select(key => key.Product).ToList();
                        var excludedVariants = offer.Where(filter => filter.Product > 0 && filter.Variant > 0 && filter.LineType == 1).Select(key => key.Variant).ToList();

                        if (category == null)
                        {
                            continue;
                        }

                        var products = productsWithBasePrices.Where(filter => filter.Category == category.RecordId);

                        foreach (var product in products)
                        {
                            if (excludedProducts.Contains(product.RecordId) || excludedVariants.Contains(product.RecordId))
                            {
                                continue;
                            }

                            if (discount.UOM == "" || discount.UOM == product.UOM)
                            {
                                var tierPrice = new ProductTierPrice()
                                {
                                    Sku = product.Sku,
                                    Quantity = Math.Round(discount.LowestQty, 2).ToString(),
                                    TierPrice = GetQuantityDiscountPrice(product.BasePrice, discount),
                                    Website = priceParams.MagentoStoreWebsite,
                                    CustomerGroup = "ALL GROUPS",
                                    ValueType = "Fixed"
                                };

                                tierPriceList.Add(tierPrice);
                            }
                        }

                        foreach (var subCategory in productCategories.Where(filter => filter.ParentCategory == category.RecordId))
                        {
                            if (!excludedCategories.Contains(subCategory.RecordId))
                            {
                                tierPriceList.AddRange(GetSubCategoryQuantityDiscounts(productCategories, priceParams, discount, productsWithBasePrices, storeId, subCategory.RecordId, excludedCategories, excludedProducts, excludedVariants));
                            }
                        }
                    }
                }
            }

            return tierPriceList;
        }

        private static List<ProductTierPrice> GetSubCategoryQuantityDiscounts(List<ProductCategoryBlob> productCategories, PriceParams priceParams, QuantityDiscount discount, List<ProductPriceItem> productsList, string storeId, long categoryId, List<long> exclusionList, List<long> excludedProducts, List<long> excludedVariants)
        {
            var tierPriceList = new List<ProductTierPrice>();

            var products = productsList.Where(filter => filter.Category == categoryId);

            foreach (var product in products)
            {
                if (excludedProducts.Contains(product.RecordId) || excludedVariants.Contains(product.RecordId))
                {
                    continue;
                }

                var tierPrice = product.BasePrice;

                if (discount.UOM == "" || discount.UOM == product.UOM)
                {
                    var tierPriceItem = new ProductTierPrice()
                    {
                        Sku = product.Sku,
                        Quantity = Convert.ToString(discount.LowestQty),
                        TierPrice = GetQuantityDiscountPrice(product.BasePrice, discount),
                        Website = priceParams.MagentoStoreWebsite,
                        CustomerGroup = "ALL GROUPS",
                        ValueType = "Fixed"
                    };

                    tierPriceList.Add(tierPriceItem);
                }
            }

            foreach (var subCategory in productCategories.Where(filter => filter.ParentCategory == categoryId))
            {
                if (!exclusionList.Contains(subCategory.RecordId))
                {
                    tierPriceList.AddRange(GetSubCategoryQuantityDiscounts(productCategories, priceParams, discount, productsList, storeId, subCategory.RecordId, exclusionList, excludedProducts, excludedVariants));
                }
            }

            return tierPriceList;
        }

        private static decimal GetQuantityDiscountPrice(decimal basePrice, QuantityDiscount qtyDiscount)
        {
            var discount = qtyDiscount.OfferPrice;

            if (qtyDiscount.Discount > 0)
            {
                discount = basePrice - (qtyDiscount.Discount / 100) * basePrice;
            }
            else if (qtyDiscount.DiscountAmount > 0)
            {
                discount = basePrice - (qtyDiscount.DiscountAmount / qtyDiscount.LowestQty);
            }

            return discount;
        }
    }
}