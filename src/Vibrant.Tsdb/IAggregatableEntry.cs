using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IAggregatableEntry : IEntry
   {
      void SetCount( int count );

      int GetCount();

      object GetField( string name );

      void SetField( string name, object value );
   }
}
