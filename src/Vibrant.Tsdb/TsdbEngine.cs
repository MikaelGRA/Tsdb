//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;

//namespace Vibrant.Tsdb
//{
//   public class TsdbEngine
//   {
//      public TsdbEngine()
//      {
//      }

//      public Task Complete( Guid id )
//      {
//         // short / medium -> long
//      }

//      public Task Store<TEntry>( IEnumerable<TEntry> items )
//         where TEntry : IEntry
//      {
//         // store in short

//         // do we need to: short -> medium / medium -> long
//      }

//      public Task<List<TEntry>> Retrieve<TEntry>( Guid id )
//         where TEntry : IEntry
//      {

//      }

//      public Task<List<TEntry>> Retrieve<TEntry>( Guid id, DateTime from, DateTime to )
//         where TEntry : IEntry
//      {

//      }
//   }
//}
