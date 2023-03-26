using Microsoft.Dynamics.Commerce.RetailProxy;
using RCK.CloudPlatform.Model.Product;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RCK.CloudPlatform.D365
{
    public class CategoryController
    {
        public static async Task<List<Category>> GetChannelCategoriesAsync(D365RetailServerContext retailServerContext, long retailServerPagingTop)
        {
            var categoryManager = retailServerContext.FactoryManager.GetManager<ICategoryManager>();

            var queryResultSettings = new QueryResultSettings
            {
                Paging = new PagingInfo
                {
                    Skip = 0,
                    Top = retailServerPagingTop
                }
            };

            var categories = await categoryManager.GetCategories(retailServerContext.BaseChannelId, queryResultSettings: queryResultSettings);

            if (categories == null || categories.Results.Count() == 0)
            {
                throw new System.Exception($"0 categories found in AX!");
            }

            return categories.Results.ToList();
        }


        public static async Task<List<ProductCategoryBlob>> GetChannelCategoriesAsync(D365RetailServerContext retailServerContext)
        {
            var categoryManager = retailServerContext.FactoryManager.GetManager<ICategoryManager>();

            var queryResultSettings = new QueryResultSettings
            {
                Paging = new PagingInfo
                {
                    Skip = 0,
                    Top = 1000
                }
            };

            var d365Categories = await categoryManager.GetCategories(retailServerContext.BaseChannelId, queryResultSettings: queryResultSettings);

            if (d365Categories == null || d365Categories.Results.Count() == 0)
            {
                throw new System.Exception($"0 categories found in AX!");
            }

            var categories = d365Categories.Select(c => new ProductCategoryBlob { Name = c.Name, ParentCategory = c.ParentCategory, RecordId = c.RecordId }).ToList();
            return categories;
            
        }
    }
}