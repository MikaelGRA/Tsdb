﻿using System;
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
         var count = reader.ReadByte();
         for( int i = 0 ; i < count ; i++ )
         {
            var key = reader.ReadString();
            var value = reader.ReadPrimitive();
            Fields.Add( key, value );
         }
      }

      public void Write( BinaryWriter writer )
      {
         // data
         writer.Write( (byte)Fields.Count );
         foreach( var field in Fields )
         {
            writer.Write( field.Key );
            writer.WritePrimitive( field.Value );
         }
      }

      void IInfluxRow<DateTime?>.SetTimestamp( DateTime? value )
      {
         if( value.HasValue )
         {
            Timestamp = value.Value;
         }
      }

      DateTime? IInfluxRow<DateTime?>.GetTimestamp()
      {
         return Timestamp;
      }

      void IInfluxRow<DateTime?>.SetField( string name, object value )
      {
         Fields[ name ] = value;
      }

      object IInfluxRow<DateTime?>.GetField( string name )
      {
         return Fields[ name ];
      }

      void IInfluxRow<DateTime?>.SetTag( string name, string value )
      {
      }

      string IInfluxRow<DateTime?>.GetTag( string name )
      {
         return null;
      }

      IEnumerable<KeyValuePair<string, string>> IInfluxRow<DateTime?>.GetAllTags()
      {
         return _empty;
      }

      IEnumerable<KeyValuePair<string, object>> IInfluxRow<DateTime?>.GetAllFields()
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
