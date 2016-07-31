using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage
   {
      Task Write( IEnumerable<IEntry> items );




      Task<int> Delete( string id, DateTime from, DateTime to );

      Task<int> Delete( string id );

      Task<ReadResult<IEntry>> ReadLatest( string id );

      Task<ReadResult<TEntry>> ReadLatestAs<TEntry>( string id ) where TEntry : IEntry;

      Task<ReadResult<IEntry>> Read( string id );

      Task<ReadResult<TEntry>> ReadAs<TEntry>( string id ) where TEntry : IEntry;

      Task<ReadResult<IEntry>> Read( string id, DateTime from, DateTime to );

      Task<ReadResult<TEntry>> ReadAs<TEntry>( string id, DateTime from, DateTime to ) where TEntry : IEntry;





      Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to );

      Task<int> Delete( IEnumerable<string> ids );

      Task<MultiReadResult<IEntry>> ReadLatest( IEnumerable<string> ids );

      Task<MultiReadResult<TEntry>> ReadLatestAs<TEntry>( IEnumerable<string> ids ) where TEntry : IEntry;

      Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids );

      Task<MultiReadResult<TEntry>> ReadAs<TEntry>( IEnumerable<string> ids ) where TEntry : IEntry;

      Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to );

      Task<MultiReadResult<TEntry>> ReadAs<TEntry>( IEnumerable<string> ids, DateTime from, DateTime to ) where TEntry : IEntry;
   }
}
