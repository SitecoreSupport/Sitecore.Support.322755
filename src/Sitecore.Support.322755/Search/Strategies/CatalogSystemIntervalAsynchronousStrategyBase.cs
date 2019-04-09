using CommerceOps.Sitecore.Commerce.Plugin.Catalog;
using Sitecore.Commerce.Engine.Connect;
using Sitecore.Commerce.Engine.Connect.DataProvider;
using Sitecore.Commerce.Engine.Connect.Events;
using Sitecore.Commerce.Engine.Connect.Search;
using Sitecore.Commerce.Plugin.ManagedLists;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.Data;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Sitecore.Support.Search.Strategies
{
  public abstract class CatalogSystemIntervalAsynchronousStrategyBase<TCatalogEntity>
    : Sitecore.Commerce.Engine.Connect.Search.Strategies.CatalogSystemIntervalAsynchronousStrategyBase<TCatalogEntity> 
    where TCatalogEntity : Sitecore.Commerce.Plugin.Catalog.CatalogItemBase
  {
    private static object mappingLock = new object();

    private MethodInfo _checkLoadMappingsFailed = typeof(Sitecore.Commerce.Engine.Connect.DataProvider.CatalogRepository)
      .GetMethod("CheckLoadMappingsFailed", BindingFlags.NonPublic | BindingFlags.Static);

    private MethodInfo _loadMappingEntries = typeof(Sitecore.Commerce.Engine.Connect.DataProvider.CatalogRepository)
      .GetMethod("LoadMappingEntries", BindingFlags.Instance | BindingFlags.NonPublic);

    public int MaxMappingReloads { get; } = 5;

    public int ItemsToIndexMinThreshold { get; } = 100;

    protected CatalogSystemIntervalAsynchronousStrategyBase(string interval, string database, string maxMappingReloads, string itemsToIndexMinThreshold) : base(interval, database)
    {
      int maxReloadsParam, itemsToIndexMinThresholdParam;
      if (int.TryParse(maxMappingReloads, out maxReloadsParam))
      {
        this.MaxMappingReloads = maxReloadsParam;
      }

      if (int.TryParse(itemsToIndexMinThreshold, out itemsToIndexMinThresholdParam))
      {
        this.ItemsToIndexMinThreshold = itemsToIndexMinThresholdParam;
      }
    }
    
    protected abstract override ManagedList GetIncrementalEntitiesToIndex(string environment, int skip);

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider", MessageId = "System.String.Format(System.String,System.Object)", Justification = "Locale is not required for log messages.")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider", MessageId = "System.String.Format(System.String,System.Object,System.Object,System.Object)", Justification = "Locale is not required for log messages.")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1305:SpecifyIFormatProvider", MessageId = "System.String.Format(System.String,System.Object,System.Object)", Justification = "Locale is not required for log messages.")]
    protected override void IndexItems()
    {
      this.LogMessage($"Checking for entities to index in list '{this.IncrementalIndexListName}'.");
      var totalCount = 0;
      var crawledArtifactStores = new List<string>();
      var catalogRepository = new CatalogRepository();

      if (string.IsNullOrWhiteSpace(this.IncrementalIndexListName))
      {
        this.LogDebug($"skipping incremental updates because no {nameof(this.IncrementalIndexListName)} value has been specified.");
        return;
      }

      try
      {
        foreach (var environment in this.Environments)
        {
          var artifactStoreId = IndexUtility.GetEnvironmentArtifactStoreId(environment);
          if (crawledArtifactStores.Contains(artifactStoreId))
          {
            return;
          }

          crawledArtifactStores.Add(artifactStoreId);

          var allIds = new List<ID>();

          ManagedList itemsList = null;
          var targetDatabase = Database.GetDatabase(this.DatabaseName);

          // 322755: initialize
          var reloadsCount = 0;
          int changesBatchCount = 0;

          do
          {
            itemsList = this.GetIncrementalEntitiesToIndex(environment, 0);

            // 322755: Stop loading changes if there are no changes or if retreived number of changes is smaller than the ItemsToIndexMinThreshold
            if (itemsList == null || (changesBatchCount > 0 && itemsList.Items.Count < this.ItemsToIndexMinThreshold))
            {
              break;
            }

            if (itemsList != null && itemsList.Items.Count > 0)
            {
              var sitecoreIdList = new List<ID>();
              var legacySitecoreIdList = new List<ID>();
              var entityIdList = new List<string>();

              bool updateOccurred = false;

              // 322755: update mappings once per items to index batch
              if (UpdateMappingEntries(catalogRepository, itemsList.Items.Max(item => item.DateUpdated).Value.UtcDateTime))
              {
                updateOccurred = true;
              }

              foreach (var entity in itemsList.Items.OfType<TCatalogEntity>())
              {
                try
                {
                  legacySitecoreIdList.Add(ID.Parse(entity.SitecoreId));
                  
                  var catalogItemDeterministicIds = catalogRepository.GetCatalogItemDeterministicIds();

                  var deterministicIdList = catalogRepository.GetDeterministicIdsForEntityId(entity.Id, false);
                  if (deterministicIdList.Count > 0)
                  {
                    entityIdList.Add(entity.Id);
                    foreach (var deterministicId in deterministicIdList)
                    {
                      if (catalogItemDeterministicIds.Contains(ID.Parse(deterministicId)))
                      {
                        sitecoreIdList.Add(ID.Parse(deterministicId));
                      }
                    }
                  }
                  else
                  {
                    this.LogDebug($"The entity '{entity.Id}' is not included in any catalog selected by a catalogs folder item.  This item will not be indexed.");
                    entityIdList.Add(entity.Id);
                  }
                }
                catch (Exception ex)
                {
                  this.LogError(ex, $"An unexpected error occurred while indexing entity '{entity.Id}' in list '{this.IncrementalIndexListName}'");
                }
              }

              // 322755: update number of reloads if an update ended up reloading mappings
              if (updateOccurred)
              {
                reloadsCount++;
              }

              // 322755: update number of batches retrieved from Commerce Engine
              changesBatchCount++;

              if (sitecoreIdList.Count > 0)
              {
                var ids = sitecoreIdList.Union(legacySitecoreIdList).ToList();

                // Ensure this item is removed from sitecore and catalog repository caches.
                foreach (var deterministicId in sitecoreIdList)
                {
                  EngineConnectUtility.RemoveItemFromSitecoreCaches(deterministicId, this.DatabaseName);
                }

                foreach (var sitecoreId in ids)
                {
                  CatalogRepository.DefaultCache.RemovePrefix(sitecoreId.Guid.ToString());
                }

                var indexIdList = sitecoreIdList.Select(id => new SitecoreItemUniqueId(new ItemUri(id, targetDatabase)));
                IndexCustodian.IncrementalUpdate(this.Index, indexIdList);

                allIds.AddRange(sitecoreIdList);
              }

              // Always remove list entities so that invalid entries are removed.
              this.RemoveIncrementalIndexListEntities(environment, this.IncrementalIndexListName, entityIdList);

              totalCount += entityIdList.Count;
            }
            // 322755: continue the loop ony if Commerce Engine has changes and MaxReloads limit is not reached
          } while ((itemsList != null && itemsList.Items.Count > 0) && (reloadsCount < this.MaxMappingReloads));

          if (allIds.Count > 0)
          {
            this.LogMessage($"Sending Event to clear cache for {allIds.Count} entities.");

            // Brodcast event to clear caches
            var indexingCompletedEvent = new IndexingCompletedEvent
            {
              DatabaseName = this.DatabaseName,
              SitecoreIds = allIds.Select(x => x.Guid.ToString()).Distinct().ToArray()
            };

            var eventQueue = new DefaultEventQueueProvider();
            eventQueue.QueueEvent(indexingCompletedEvent, true, true);
          }
        }
      }
      finally
      {
        this.LogMessage($"indexed {totalCount} entities in list '{this.IncrementalIndexListName}'.");
      }
    }

    protected virtual bool UpdateMappingEntries(CatalogRepository catalogRepository, DateTime? requiredUpdateTimeUtc = default(DateTime?))
    {
      var reloadOccurred = false;
      bool flag = requiredUpdateTimeUtc.HasValue && CatalogRepository.MappingEntriesLastUpdatedUtc.HasValue && requiredUpdateTimeUtc > CatalogRepository.MappingEntriesLastUpdatedUtc;
      if (((CatalogRepository.MappingEntries == null) | flag) || (bool)_checkLoadMappingsFailed.Invoke(null, new object[] { }))
      {
        Log.Info("Commerce.Connector - Acquiring mapping lock", this);
        lock (mappingLock)
        {
          Log.Info("Commerce.Connector - Mapping locked", this);
          flag = (requiredUpdateTimeUtc.HasValue && CatalogRepository.MappingEntriesLastUpdatedUtc.HasValue && requiredUpdateTimeUtc > CatalogRepository.MappingEntriesLastUpdatedUtc);
          if (((CatalogRepository.MappingEntries == null) | flag) || (bool)_checkLoadMappingsFailed.Invoke(null, new object[] { }))
          {
            _loadMappingEntries.Invoke(catalogRepository, new object[] { });
            reloadOccurred = true;
          }

          Log.Info("Commerce.Connector - Release mapping lock", this);
        }
      }

      return reloadOccurred;
    }
  }
}