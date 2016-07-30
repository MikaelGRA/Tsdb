using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static async Task<int> DeleteMulti( this IVolumeStorage storage, IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.Delete( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Sum( x => x.Result );
      }

      public static async Task<int> DeleteMulti( this IVolumeStorage storage, IEnumerable<string> ids )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.Delete( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Sum( x => x.Result );
      }

      public static async Task<MultiReadResult<IEntry>> ReadLatestMulti( this IVolumeStorage storage, IEnumerable<string> ids )
      {
         var tasks = new List<Task<ReadResult<IEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.ReadLatest( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<IEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }

      public static async Task<MultiReadResult<TEntry>> ReadLatestMultiAs<TEntry>( this IVolumeStorage storage, IEnumerable<string> ids ) where TEntry : IEntry
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.ReadLatestAs<TEntry>( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }

      public static async Task<MultiReadResult<IEntry>> ReadMulti( this IVolumeStorage storage, IEnumerable<string> ids )
      {
         var tasks = new List<Task<ReadResult<IEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.Read( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<IEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }

      public static async Task<MultiReadResult<TEntry>> ReadMultiAs<TEntry>( this IVolumeStorage storage, IEnumerable<string> ids ) where TEntry : IEntry
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.ReadAs<TEntry>( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }

      public static async Task<MultiReadResult<IEntry>> ReadMulti( this IVolumeStorage storage, IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<ReadResult<IEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.Read( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<IEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }

      public static async Task<MultiReadResult<TEntry>> ReadMultiAs<TEntry>( this IVolumeStorage storage, IEnumerable<string> ids, DateTime from, DateTime to ) where TEntry : IEntry
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( storage.ReadAs<TEntry>( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TEntry>( tasks.ToDictionary( x => x.Result.Id, x => x.Result ) );
      }
   }
}
