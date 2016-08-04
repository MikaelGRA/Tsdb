using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   internal class AtsQueryResult<TEntry>
      where TEntry : IAtsEntry
   {
      private TEntry[] _entries;
      private Sort _sort;

      public AtsQueryResult( TsdbTableEntity row, Sort sort )
      {
         Row = row;
         _sort = sort;
      }

      public TsdbTableEntity Row { get; private set; }

      public TEntry[] Entries
      {
         get
         {
            return _entries ?? ( _entries = Row.GetEntries<TEntry>( _sort ) );
         }
      }
   }
}
