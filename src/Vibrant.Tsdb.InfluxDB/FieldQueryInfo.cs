using System;
using System.Collections.Generic;
using System.Text;

namespace Vibrant.Tsdb.InfluxDB
{
   public class FieldQueryInfo
   {
      public FieldQueryInfo( AggregatedField aggregatedField, IFieldInfo fieldInfo )
      {
         AggregatedField = aggregatedField;
         FieldInfo = fieldInfo;
      }

      public AggregatedField AggregatedField { get; private set; }

      public IFieldInfo FieldInfo { get; private set; }
   }
}
