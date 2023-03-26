using Microsoft.ApplicationInsights;
using Microsoft.Dynamics.Commerce.RetailProxy;
using Newtonsoft.Json;
using RCK.CloudPlatform.Common.ExtensionMethods;
using RCK.CloudPlatform.Model.ERP;
using RCK.CloudPlatform.Model.Product;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VSI.CloudPlatform.Common;
using VSI.CloudPlatform.Model.Common;
using VSI_RT = VSI.Commerce.RetailProxy;

namespace RCK.CloudPlatform.D365
{
    public class ProductController
    {
        public static D365RetailServerContext retailContext;
        private static Dictionary<long?, string> categoriesDictionary;

        public async static Task<(List<ProductEntity>, List<ProductCategoryBlob>)> GetProductsAsync(ProductParams requestParams, TelemetryClient telemetryClient, D365RetailConnectivity d365RetailConnectivity, string functionName)
        {
            var productManager = retailContext.FactoryManager.GetManager<IProductManager>();
            var vsiProductManager = retailContext.FactoryManager.GetManager<VSI_RT.IProductManager>();
            var listOfProducts = new List<ProductEntity>();
            var exceptions = new ConcurrentQueue<Exception>();

            telemetryClient.TrackTrace(functionName, $"Getting channel categories from Retail.");

            var categories = await CategoryController.GetChannelCategoriesAsync(retailContext, requestParams.CategoryPagingTop);

            categoriesDictionary = BuildCategoryHierachy(categories);

            telemetryClient.TrackTrace(functionName, $"Getting items by categories from SP.");

            var vsiItems = await vsiProductManager.VSI_GetProductItemIdsByCategory(retailContext.BaseChannelId, categories.Select(c => c.RecordId).ToList());

            telemetryClient.TrackTrace(functionName, $"Total {vsiItems.Items.Count()} Items fetched from SP by categories.");

            var items = vsiItems.Items.DistinctBy(i => i.ItemId).ToList();

            telemetryClient.TrackTrace(functionName, $"Distinct items: {items.Count()}");

            telemetryClient.TrackTrace(functionName, $"Getting assortments products from Retail.");

            Parallel.ForEach(items, new ParallelOptions
            {
                MaxDegreeOfParallelism = requestParams.MaxDegreeOfParallelism
            },
            (item) =>
            {
                telemetryClient.AddCustomProperty("ITEM_ID", item.ItemId);

                try
                {
                    SearchProductsFromD365(requestParams, telemetryClient, functionName, productManager, listOfProducts, item, vsiProductManager, categories);
                }
                catch (Exception ex)
                {
                    telemetryClient.TrackException(ex);

                    if (ex is CommunicationException || ex is UserAuthenticationException || ex is InvalidOperationException)
                    {
                        telemetryClient.TrackTrace(functionName, $"Resetting Retail Server Connection...");

                        retailContext = RetailConnectivityController.ConnectToRetailServerAsync(d365RetailConnectivity).GetAwaiter().GetResult();

                        telemetryClient.TrackTrace(functionName, $"Resetting Retail Server Connection Success.");

                        productManager = retailContext.FactoryManager.GetManager<IProductManager>();
                        vsiProductManager = retailContext.FactoryManager.GetManager<VSI_RT.IProductManager>();

                        SearchProductsFromD365(requestParams, telemetryClient, functionName, productManager, listOfProducts, item, vsiProductManager, categories);
                    }
                }
            });

            if (exceptions.Distinct().Count() > 0)
            {
                throw new AggregateException(exceptions);
            }

            return (listOfProducts, categories.Select(c => new ProductCategoryBlob { RecordId = c.RecordId, ParentCategory = c.ParentCategory, Name = c.Name }).ToList());
        }

        private static void SearchProductsFromD365(ProductParams requestParams, TelemetryClient telemetryClient, string functionName, IProductManager productManager, List<ProductEntity> listOfProducts, VSI_RT.VSIItem item, VSI_RT.IProductManager vsiProductManager, List<Category> categories)
        {
            var productPaging = new QueryResultSettings { Paging = new PagingInfo { Skip = 0, Top = requestParams.ProductPagingTop } };
            var productCriteria = new ProductSearchCriteria
            {
                Context = new ProjectionDomain
                {
                    ChannelId = retailContext.BaseChannelId
                },
                ItemIds = new ObservableCollection<ProductLookupClause> { new ProductLookupClause { ItemId = item.ItemId } },
                DataLevelValue = (int)CommerceEntityDataLevel.Minimal
            };

            var products = productManager.Search(productCriteria, productPaging).GetAwaiter().GetResult();

            if (products.Count() == 0)
            {
                telemetryClient.TrackTrace(functionName, $"Search result is 0 : {item.ItemId}");
            }

            if (products.Contains(null))
            {
                telemetryClient.TrackTrace(functionName, $"Search result contains NULL : {item.ItemId}");
            }

            if (products.Count() > 0)
            {
                var erpProducts = MapToProductEntities(products.Results, item.CategoryIds.FirstOrDefault(), vsiProductManager, requestParams, categories);

                if (erpProducts.Count == 0)
                {
                    telemetryClient.TrackTrace(functionName, $"MAPPED LIST IS 0 : {item.ItemId}");
                }

                for (int k = 0; k < erpProducts.Count; k++)
                {
                    if (erpProducts[k] != null)
                    {
                        listOfProducts.Add(erpProducts[k]);
                    }
                    else
                    {
                        telemetryClient.TrackTrace(functionName, $"NULL PRODUCTS : {item.ItemId}");
                    }
                }
            }
        }

        private static IEnumerable<VSI.Commerce.RetailProxy.VSIRocklerCustomProductAttribute> GetRocklerProductCustomAttributesFromD365(List<string> itemIds, VSI_RT.IProductManager productManager)
        {
            var response = productManager.VSI_GetRocklerProductCustomAttributes(itemIds, retailContext.BaseChannelId).GetAwaiter().GetResult();

            return response.CustomAttributes;
        }

        private static List<ProductEntity> MapToProductEntities(IEnumerable<Product> products, long categoryId, VSI_RT.IProductManager productManager, ProductParams requestParams, List<Category> categories)
        {
            var list = new List<ProductEntity>();

            var erpProducts = JsonConvert.DeserializeObject<List<ErpProduct>>(JsonConvert.SerializeObject(products));

            foreach (var product in erpProducts)
            {
                product.LanguageCode = retailContext.DefaultLanguage.Split('-')[0];
                product.Language = retailContext.DefaultLanguage;

                var name = GetCustomAttributeValue(product, "en-CA", "ProductName");
                var description = GetCustomAttributeValue(product, "en-CA", "Description");

                if (!string.IsNullOrEmpty(name)) product.ProductName = name;
                if (!string.IsNullOrEmpty(description)) product.Description = description;
            }

            var productsCustomAttributes = GetRocklerProductCustomAttributesFromD365(erpProducts.Select(ep => ep.ProductNumber).ToList(), productManager);

            var variantsAsErpProducts = ConvertVariantsAsErpProducts(erpProducts.Where(p => p.IsMasterProduct).ToList());

            erpProducts.AddRange(variantsAsErpProducts);

            foreach (var product in erpProducts)
            {
                var categoryText = GetCategoryHirarchy(categoryId);
                //var categoryText = string.Empty;

                //if (categoryIds.Count == 1)
                //{
                //    categoryText += GetCategoryHirarchy(categoryIds.FirstOrDefault());
                //}
                //else if (categoryIds.Count > 1)
                //{
                //    var last = categoryIds.Last();
                //    foreach (var categoryId in categoryIds)
                //    {
                //        if (categoryId != last)
                //        {
                //            categoryText += GetCategoryHirarchy(categoryId) + "|";
                //        }
                //        else
                //        {
                //            categoryText += GetCategoryHirarchy(categoryId);
                //        }
                //    }
                //}

                var additionalAttributes = new StringBuilder();

                var pa = ProductAdditionalAttributes(categoryId, categories, productsCustomAttributes, product, additionalAttributes);

                var erpProduct = JsonConvert.DeserializeObject<ErpProduct>(JsonConvert.SerializeObject(product));

                var entity = new ProductEntity
                {
                    RecordId = product.RecordId,
                    CategoryId = categoryId,
                    sku = product.ProductNumber,
                    description = product.Description,
                    //name = product.ProductName,
                    language_code = product.LanguageCode,
                    attribute_set_code = requestParams.AttributeSetCode,
                    product_websites = requestParams.ProductWebsite,
                    product_type = product.IsMasterProduct ? "configurable" : "simple",
                    //categories = categoryText.TrimEnd(new char[] { '|' }),
                    short_description = product.Description,
                    additional_attributes = $"{additionalAttributes}",
                    unit_of_measure = product.Rules?.DefaultUnitOfMeasure ?? string.Empty,
                    multiple_shipping_group = string.IsNullOrWhiteSpace(pa?.ListId) ? "" : pa?.ListId
                };

                list.Add(entity);
            }

            return list;
        }

        private static VSI_RT.VSIRocklerCustomProductAttribute ProductAdditionalAttributes(long categoryId, List<Category> categories, IEnumerable<VSI_RT.VSIRocklerCustomProductAttribute> productsCustomAttributes, ErpProduct product, StringBuilder additionalAttributes)
        {
            var pa = productsCustomAttributes.FirstOrDefault(ca => ca.ItemId == product.ProductNumber);

            var daxCategoryLine = categories.FirstOrDefault(c => c.RecordId == categoryId);

            var daxFamilyCategoryName = string.Empty;
            var daxLineCategoryName = string.Empty;
            var daxCategoryName = string.Empty;

            if (daxCategoryLine != null)
            {
                daxLineCategoryName = daxCategoryLine.Name;

                if (daxCategoryLine.ParentCategory.GetValueOrDefault() > 0)
                {
                    var daxCategory = categories.FirstOrDefault(c => c.RecordId == daxCategoryLine.ParentCategory.Value);
                    if (daxCategory != null)
                    {
                        daxCategoryName = daxCategory.Name;
                    }

                    if (daxCategory.ParentCategory.GetValueOrDefault() > 0)
                    {
                        var daxFamily = categories.FirstOrDefault(c => c.RecordId == daxCategory.ParentCategory.Value);
                        if (daxFamily != null)
                        {
                            daxFamilyCategoryName = daxFamily.Name;
                        }
                    }
                }
            }

            additionalAttributes.Append($"backorder_date={((pa?.ATPDate.Date == default(DateTime)) ? "" : pa?.ATPDate.Date.ToShortDateString())},");
            additionalAttributes.Append($"cost={pa?.Price},");
            additionalAttributes.Append($"country_of_manufacture={pa?.CountryOriginRegionId},");
            additionalAttributes.Append($"direct_ship={((pa?.Dropshipment != null && pa?.Dropshipment == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"discountable={((pa?.EndDisc != null && pa?.EndDisc == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"exclusive={((pa?.ProprietaryType != null && pa?.ProprietaryType == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"export_eligible={((pa?.ExportEligible != null && pa?.ExportEligible == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"extra_shipping_cost={pa?.MarkupGroupId},");
            additionalAttributes.Append($"iparcel_height={pa?.Height},");
            additionalAttributes.Append($"hts_code={pa?.HTSCode},");
            additionalAttributes.Append($"iparcel_length={pa?.Depth},");
            additionalAttributes.Append($"ship_alone={((pa?.ShipAlone != null && pa?.ShipAlone == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"weight={pa?.NetWeight},");
            additionalAttributes.Append($"iparcel_width={pa?.Width},");
            additionalAttributes.Append($"ormd={((pa?.HmimIndicator != null && pa?.HmimIndicator == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"use_config_backorders={pa?.NoBackOrder},");
            additionalAttributes.Append($"top_seller={((pa?.Top200 != null && pa?.Top200 == 1) ? "Yes" : "No")},");
            additionalAttributes.Append($"dax_californiaprop65={pa?.Prop65Script},");
            additionalAttributes.Append($"dax_status={pa?.StatusCode},");
            additionalAttributes.Append($"warrantable_item={pa?.BWGWarrantableFlag},");
            additionalAttributes.Append($"dax_taxonomy_family={daxFamilyCategoryName},");
            additionalAttributes.Append($"dax_taxonomy_category={daxCategoryName},");
            additionalAttributes.Append($"dax_taxonomy_line={daxLineCategoryName},");
            //additionalAttributes.Append($"multiple_shipping_group={pa?.ListId},");
            additionalAttributes.Append($"eligible_for_promo=No");
            return pa;
        }

        private static List<ErpProduct> ConvertVariantsAsErpProducts(List<ErpProduct> erpMasterProducts)
        {
            var erpProducts = new List<ErpProduct>();

            foreach (var erpMasterProduct in erpMasterProducts)
            {
                var variants = erpMasterProduct?.CompositionInformation?.VariantInformation?.Variants;

                if (variants == null)
                {
                    continue;
                }

                foreach (var variant in variants)
                {
                    var erpProduct = new ErpProduct
                    {
                        Mode = erpMasterProduct.Mode,
                        CustomAttributes = variant.CustomAttributes,
                        AdjustedPrice = variant.AdjustedPrice,
                        BasePrice = variant.BasePrice,
                        ItemId = variant.ItemId,
                        VariantItemId = variant.VariantId,
                        ConfigId = variant.ConfigId,
                        Configuration = variant.Configuration,
                        RecordId = variant.DistinctProductVariantId,
                        DistinctProductVariantId = variant.DistinctProductVariantId,
                        InventoryDimensionId = variant.InventoryDimensionId,
                        IsMasterProduct = false,
                        MasterProductId = variant.MasterProductId,
                        MasterProductNumber = erpMasterProduct.ProductNumber,
                        ProductProperties = JsonConvert.DeserializeObject<List<ErpProductPropertyTranslation>>(JsonConvert.SerializeObject(variant.PropertiesAsList)),
                        ProductName = GetVariantValue(variant, "en-CA", "ProductName"),
                        Description = GetVariantValue(variant, "en-CA", "Description"),
                        Price = variant.Price,
                        ProductNumber = variant.ProductNumber,
                        ImageList = erpMasterProduct.ImageList,
                        Size = variant.Size,
                        SizeId = variant.SizeId,
                        Style = variant.Style,
                        StyleId = variant.StyleId,
                        VariantId = variant.VariantId,
                        EcomProductId = variant.ItemId,
                        Status = erpMasterProduct.Status,
                        CatalogId = erpMasterProduct.CatalogId,
                        Color = variant.Color,
                        ColorId = variant.ColorId,
                        CategoryIds = erpMasterProduct.CategoryIds,
                        Barcode = variant.Barcode,
                        TaxPercentage = variant.TaxPercentage,
                        Language = retailContext.DefaultLanguage,
                        LanguageCode = retailContext.DefaultLanguage.Split('-')[0],
                        Rules = erpMasterProduct.Rules
                    };

                    erpProducts.Add(erpProduct);
                }
            }

            return erpProducts;
        }

        private static string GetCustomAttributeValue(ErpProduct erpProduct, string language, string attributeKey)
        {
            return GetCustomTranslatedAttributeValue(erpProduct, language, attributeKey) ?? GetCustomNumericAttributeValue(erpProduct, attributeKey);
        }

        private static string GetCustomTranslatedAttributeValue(ErpProduct erpProduct, string language, string attributeKey)
        {
            var productProperty = erpProduct.ProductProperties?.Where(p => p.TranslationLanguage.Equals(language, StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();

            if (productProperty == null)
            {
                return string.Empty;
            }

            var translatedProperty = productProperty.TranslatedProperties?.Where(t => t.KeyName == attributeKey || t.FriendlyName == attributeKey || t.AttributeValueId.ToString() == attributeKey)?.FirstOrDefault();

            if (translatedProperty == null)
            {
                return null;
            }

            return translatedProperty.ValueString;
        }

        private static string GetVariantValue(ErpProductVariant erpProduct, string language, string attributeKey)
        {
            var productProperty = erpProduct.PropertiesAsList?.Where(p => p.TranslationLanguage.Equals(language, StringComparison.OrdinalIgnoreCase))?.FirstOrDefault();

            if (productProperty == null)
            {
                return string.Empty;
            }

            var translatedProperty = productProperty.TranslatedProperties?.Where(t => t.KeyName == attributeKey)?.FirstOrDefault();

            if (translatedProperty == null)
            {
                return null;
            }

            return translatedProperty.ValueString;
        }

        private static string GetCustomNumericAttributeValue(ErpProduct erpProduct, string attributeKey)
        {
            var productNumericProperty = erpProduct.ProductProperties?.Where(p => p.TranslationLanguage.ToLower() == "en-us")?.FirstOrDefault();

            if (productNumericProperty == null)
            {
                return string.Empty;
            }

            var numericProperty = productNumericProperty.TranslatedProperties?.Where(t => t.KeyName == attributeKey)?.FirstOrDefault();

            return numericProperty?.ValueString ?? string.Empty;
        }

        private static Dictionary<long?, string> BuildCategoryHierachy(List<Category> categories)
        {
            var categoryHirarchy = new Dictionary<long?, string>();
            var tempList = new List<string>();

            foreach (var category in categories)
            {
                var categoryName = category.NameTranslations.FirstOrDefault();
                if (categoryName != null)
                {
                    tempList.Add(categoryName.Text);
                }
                

                var parentCategoryId = category.ParentCategory;

                while (parentCategoryId.GetValueOrDefault() > 0)
                {
                    var parentCategory = categories.FirstOrDefault(key => key.RecordId == parentCategoryId);
                    var parentCategoryName = parentCategory.NameTranslations.FirstOrDefault();
                    if (parentCategoryName != null)
                    {
                        tempList.Add(parentCategoryName.Text == "Rockler Product Hierarchy" ? "Default Category": parentCategoryName.Text);
                    }
                    

                    parentCategoryId = parentCategory.ParentCategory;
                }

                tempList.Reverse();
                //tempList.Insert(0, "Default Category");

                categoryHirarchy[category.RecordId] = string.Join("/", tempList);

                tempList.Clear();
            }

            return categoryHirarchy;
        }

        private static string GetCategoryHirarchy(long categoryId)
        {
            var categoryText = string.Empty;

            if (categoriesDictionary.ContainsKey(categoryId))
            {
                categoryText = categoriesDictionary[categoryId];
            }

            return categoryText;
        }

        public static async Task<List<ProductBlob>> GetProductIdsAsync(D365RetailServerContext retailServerContext)
        {
            var productManager = retailServerContext.FactoryManager.GetManager<VSI_RT.IProductManager>();

            var queryResultSettings = new QueryResultSettings
            {
                Paging = new PagingInfo
                {
                    Skip = 0,
                    Top = 2000
                }
            };

            var response = await productManager.VSI_GetProductIds(retailServerContext.BaseChannelId);

            if (response.Products == null)
            {
                throw new System.Exception($"0 Products found in AX!");
            }

            var products = response.Products.Select(p => new ProductBlob { Id = p.Product, SKU = p.ItemId, UOM = p.UOM, CategoryId = p.Category }).ToList();
            return products;

        }
    }
}