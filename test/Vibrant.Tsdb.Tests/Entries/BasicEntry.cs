using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Files;
using Vibrant.Tsdb.InfluxDB;
using Vibrant.Tsdb.Redis;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Tests.Entries
{
   public class BasicEntry : IEntry, IAtsEntry, ISqlEntry, IRedisEntry, IInfluxEntry, IFileEntry, IAggregatableEntry
   {
      private static readonly KeyValuePair<string, string>[] _empty = new KeyValuePair<string, string>[ 0 ];

      public BasicEntry()
      {
         Fields = new SortedDictionary<string, object>( StringComparer.Ordinal );
      }

      public DateTime Timestamp { get; set; }

      public double Value
      {
         get
         {
            object value;
            if( Fields.TryGetValue( "Value", out value ) )
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

      public DateTime GetTimestamp()
      {
         return Timestamp;
      }

      public void SetTimestamp( DateTime timestamp )
      {
         Timestamp = timestamp;
      }

      public void Read( BinaryReader reader )
      {
         Value = reader.ReadDouble();
      }

      public void Write( BinaryWriter writer )
      {
         writer.Write( Value );
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

      void IAggregatableEntry.SetCount( int count )
      {
         Fields[ "Count" ] = count;
      }

      int IAggregatableEntry.GetCount()
      {
         object count;
         if( !Fields.TryGetValue( "Count", out count ) )
         {
            return 1;
         }
         return (int)count;
      }

      object IAggregatableEntry.GetField( string name )
      {
         return Fields[ name ];
      }

      void IAggregatableEntry.SetField( string name, object value )
      {
         Fields[ name ] = value;
      }

      DateTime IEntry.GetTimestamp()
      {
         return Timestamp;
      }

      void IEntry.SetTimestamp( DateTime timestamp )
      {
         Timestamp = timestamp;
      }

      #endregion
   }
}
