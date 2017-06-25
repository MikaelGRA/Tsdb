using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class FieldInfo : IFieldInfo
   {
      public FieldInfo( string key, Type valueType )
      {
         Key = key;
         ValueType = valueType;
      }

      public string Key { get; private set; }

      public Type ValueType { get; private set; }
   }
}
