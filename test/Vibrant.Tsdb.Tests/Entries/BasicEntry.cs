using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using Vibrant.Tsdb.Files;
using Vibrant.Tsdb.InfluxDB;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Ats.Tests.Entries
{
   public class BasicEntry : IEntry<string>, IAtsEntry<string>, ISqlEntry<string>, IRedisEntry<string>, IInfluxEntry<string>, IFileEntry<string>
   {
      private static readonly KeyValuePair<string, string>[] _empty = new KeyValuePair<string, string>[ 0 ];

      public BasicEntry()
      {
         Fields = new SortedDictionary<string, object>( StringComparer.Ordinal );
      }

      public string Id { get; set; }
      
      public DateTime Timestamp { get; set; }

      public double Value
      {
         get
         {
            object value;
            if(Fields.TryGetValue("Value", out value ) )
            {
               return (double)value;
            }
            return default( double );
         }
         set
         {
            Fields[ "Value" ] = value;
         }
      }

      public IDictionary<string, object> Fields { get; private set; }

      #region Interfaces

      public string GetKey()
      {
         return Id;
      }

      public void SetKey( string id )
      {
         Id = id;
      }

      public DateTime GetTimestamp()
      {
         return Timestamp;
      }

      public void SetTimestamp( DateTime timestamp )
      {
         Timestamp = timestamp;
      }

      public ushort GetTypeCode()
      {
         return 1;
      }

      public void Read( BinaryReader reader )
      {
         Value = reader.ReadDouble();
      }

      public void Write( BinaryWriter writer )
      {
         writer.Write( Value );
      }
      
      string IHaveMeasurementName.MeasurementName
      {
         get
         {
            return Id;
         }
         set
         {
            Id = value;
         }
      }

      void IInfluxRow.SetTimestamp( DateTime? value )
      {
         if( value.HasValue )
         {
            Timestamp = value.Value;
         }
      }

      DateTime? IInfluxRow.GetTimestamp()
      {
         return Timestamp;
      }

      void IInfluxRow.SetField( string name, object value )
      {
         Fields[ name ] = value;
      }

      object IInfluxRow.GetField( string name )
      {
         return Fields[ name ];
      }

      void IInfluxRow.SetTag( string name, string value )
      {
      }

      string IInfluxRow.GetTag( string name )
      {
         return null;
      }

      IEnumerable<KeyValuePair<string, string>> IInfluxRow.GetAllTags()
      {
         return _empty;
      }

      IEnumerable<KeyValuePair<string, object>> IInfluxRow.GetAllFields()
      {
         return Fields;
      }

      #endregion
   }
}
