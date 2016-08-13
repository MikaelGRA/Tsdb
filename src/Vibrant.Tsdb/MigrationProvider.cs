using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MigrationProvider<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      private IDynamicStorageSelector<TKey, TEntry> _dynamics;
      private IVolumeStorageSelector<TKey, TEntry> _volumes;

      public MigrationProvider(
         IDynamicStorageSelector<TKey, TEntry> dynamics,
         IVolumeStorageSelector<TKey, TEntry> volumes )
      {
         _dynamics = dynamics;
         _volumes = volumes;
      }

      public IEnumerable<MigrationStorages<TKey, TEntry>> Provide( TKey id, DateTime? minFrom, DateTime? maxTo )
      {
         var volumeEnumerator = _volumes.GetStorage( id, minFrom, maxTo ).GetEnumerator();
         var dynamicEnumerator = _dynamics.GetStorage( id, minFrom, maxTo ).GetEnumerator();
         bool hasMoreDynamics = dynamicEnumerator.MoveNext();

         // TODO: Could this be simplified to simply compare the returned results?

         while( volumeEnumerator.MoveNext() )
         {
            var volume = volumeEnumerator.Current;

            while( hasMoreDynamics )
            {
               var dynamic = dynamicEnumerator.Current;

               DateTime? to = null;
               if( !dynamic.To.HasValue && !volume.To.HasValue )
               {
                  to = null;
               }
               else if( dynamic.To.HasValue )
               {
                  to = dynamic.To;
               }
               else if( volume.To.HasValue )
               {
                  to = volume.To;
               }
               else if( volume.To <= dynamic.To )
               {
                  to = volume.To;
               }
               else if( dynamic.To <= volume.To )
               {
                  to = dynamic.To;
               }

               if( maxTo.HasValue && ( !to.HasValue || to > maxTo ) )
               {
                  to = maxTo;
               }

               DateTime? from = null;
               if( !dynamic.From.HasValue && !volume.From.HasValue )
               {
                  from = null;
               }
               else if( dynamic.From.HasValue )
               {
                  from = dynamic.From;
               }
               else if( volume.From.HasValue )
               {
                  from = volume.From;
               }
               if( volume.From >= dynamic.From )
               {
                  from = volume.From;
               }
               else if( dynamic.From >= volume.From )
               {
                  from = dynamic.From;
               }

               if( minFrom.HasValue && ( !from.HasValue || from < minFrom ) )
               {
                  from = minFrom;
               }



               if( ( volume.From ?? DateTime.MinValue ) < ( dynamic.To ?? DateTime.MaxValue )
                  && ( dynamic.From ?? DateTime.MinValue ) < ( volume.To ?? DateTime.MaxValue ) )
               {
                  yield return new MigrationStorages<TKey, TEntry>( dynamic.Storage, volume.Storage, from, to );
               }


               if( dynamic.From == volume.From )
               {
                  // move to next volume AND next dynamic
                  hasMoreDynamics = dynamicEnumerator.MoveNext();

                  break; // to next volume
               }
               else if( dynamic.From < volume.From )
               {
                  // dynamic should be used for potentially more volume storages

                  break; // to next volume
               }
               else // if( volume.From > dynamic.From )
               {
                  // this volume storages should consider more dynamic storages

                  hasMoreDynamics = dynamicEnumerator.MoveNext();
               }
            }
         }
      }
   }
}
