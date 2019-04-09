using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Sitecore.Commerce.Engine.Connect.Search;
using Sitecore.Commerce.Plugin.Catalog;
using Sitecore.Commerce.Plugin.ManagedLists;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Search.Strategies
{
  public class CatalogsIntervalAsynchronousStrategy : CatalogSystemIntervalAsynchronousStrategyBase<Catalog>
  {
    public CatalogsIntervalAsynchronousStrategy(string interval, string database, string maxMappingReloads, string itemsToIndexMinThreshold) 
      : base(interval, database, maxMappingReloads, itemsToIndexMinThreshold)
    {
    }

    protected override ManagedList GetIncrementalEntitiesToIndex(string environment, int skip)
    {
      Assert.ArgumentNotNullOrEmpty(environment, "environment");
      return IndexUtility.GetCatalogsToIndex(environment, base.IncrementalIndexListName, skip, base.ItemsToTake);
    }
  }
}