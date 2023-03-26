using Microsoft.Dynamics.Commerce.RetailProxy;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common.ExtensionMethods;
using RCK.CloudPlatform.D365;
using RCK.CloudPlatform.Model.Price;
using RCK.CloudPlatform.Model.Product;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using VSI_RP = VSI.Commerce.RetailProxy;

namespace RCK.CloudPlatform.AXD365
{
    public class PriceController
    {
        public static List<ProductPriceItem> FetchBasePrices(PriceParams requestParams, D365RetailServerContext retailContext, IEnumerable<ProductBlob> products)
        {
            var exceptions = new ConcurrentQueue<Exception>();
            var priceList = new ConcurrentBag<ProductPriceItem>();
            var context = new ProjectionDomain { ChannelId = retailContext.BaseChannelId };
            var productPaging = new QueryResultSettings
            {
                Paging = new PagingInfo
                {
                    Skip = 0,
                    Top = requestParams.BatchSize
                }
            };

            var manager = retailContext.FactoryManager.GetManager<IProductManager>();

            var batches = products.Batch(requestParams.BatchSize);

            Parallel.ForEach(batches, new ParallelOptions() { MaxDegreeOfParallelism = requestParams.MaxDegreeOfParallelism }, (batch =>
            {
                try
                {
                    var productBatch = batch.Select(p => p.Id).ToList();

                    var productPrices = GetBasePricesFromD365(requestParams, productBatch, context, productPaging, manager);

                    foreach (var product in batch)
                    {
                        var price = Enumerable.FirstOrDefault(productPrices, (filter => (filter.ProductId == product.Id)));
                        if (price != null)
                        {
                            var productItemPrice = new ProductPriceItem()
                            {
                                Sku = product.SKU,
                                BasePrice = GetBasePriceValue(price),
                                UOM = product.UOM,
                                Category = product.CategoryId,
                                RecordId = product.Id
                            };

                            priceList.Add(productItemPrice);
                        }
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            }));

            return priceList.ToList();
        }

        public static List<SimpleDiscount> FetchSimpleDiscounts(string channel, D365RetailServerContext retailContext)
        {
            var productManager = retailContext.FactoryManager.GetManager<VSI_RP.IProductManager>();

            var simpleDiscountResponse = productManager.VSI_GetSimpleDiscounts(channel).GetAwaiter().GetResult();

            return JsonConvert.DeserializeObject<List<SimpleDiscount>>(JsonConvert.SerializeObject(simpleDiscountResponse.SimpleDiscounts.ToList()));
        }

        public static List<MixAndMatchDealPrice> FetchMixAndMatchDealPrices(string channel, D365RetailServerContext retailContext)
        {
            var productManager = retailContext.FactoryManager.GetManager<VSI_RP.IProductManager>();

            var dealPricesResponse = productManager.VSI_GetMixAndMatchDealPrices(channel).GetAwaiter().GetResult();

            return JsonConvert.DeserializeObject<List<MixAndMatchDealPrice>>(JsonConvert.SerializeObject(dealPricesResponse.MixAndMatchDealPrices.ToList()));
        }

        public static List<QuantityDiscount> FetchQuantityDiscounts(string channel, D365RetailServerContext retailContext)
        {
            var productManager = retailContext.FactoryManager.GetManager<VSI_RP.IProductManager>();

            var quantityDiscountResponse = productManager.VSI_GetQuantityDiscounts(channel).GetAwaiter().GetResult();

            return JsonConvert.DeserializeObject<List<QuantityDiscount>>(JsonConvert.SerializeObject(quantityDiscountResponse.QuantityDiscounts.ToList()));
        }

        private static List<ProductPrice> GetBasePricesFromD365(PriceParams requestParams, List<long> productBatch, ProjectionDomain context, QueryResultSettings productPaging, IProductManager manager)
        {
            var listOfProductPrices = new List<ProductPrice>();

            var productPrices = manager.GetActivePrices(context, productBatch, requestParams.FromDate, null, new List<AffiliationLoyaltyTier>(), true, queryResultSettings: productPaging).GetAwaiter().GetResult();

            if (productPrices != null && productPrices.Results.Any())
            {
                listOfProductPrices.AddRange(productPrices.Results);

                var missingPriceProducts = productBatch.Where(pb => !productPrices.Results.Any(pp => pb == pp.ProductId))?.ToList();

                if (missingPriceProducts != null && missingPriceProducts.Count > 0)
                {
                    var missingPrices = GetBasePricesFromD365(requestParams, missingPriceProducts, context, productPaging, manager);

                    listOfProductPrices.AddRange(missingPrices);
                }
            }

            return listOfProductPrices;
        }

        private static decimal GetBasePriceValue(ProductPrice productPrice)
        {
            var result = productPrice.TradeAgreementPrice.GetValueOrDefault();

            if (result <= 0)
            {
                result = productPrice.BasePrice.GetValueOrDefault();
            }

            return Math.Round(result, 2);
        }
    }
}