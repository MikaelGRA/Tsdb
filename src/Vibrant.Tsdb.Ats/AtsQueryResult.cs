using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class AtsQueryResult
   {
      private List<IEntry> _entries;

      public AtsQueryResult( TsdbTableEntity row )
      {
         Row = row;
      }

      public TsdbTableEntity Row { get; private set; }

      public List<IEntry> Entries
      {
         get
         {
            return _entries ?? ( _entries = Row.GetEntries() );
         }
      }
   }
}
