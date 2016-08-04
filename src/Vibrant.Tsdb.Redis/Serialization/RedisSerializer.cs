using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vibrant.Tsdb.Redis;

namespace Vibrant.Tsdb.Ats.Serialization
{
   internal static class RedisSerializer
   {
      public static BinaryReader CreateReader( Stream stream )
      {
         var reader = new BinaryReader( stream, Encoding.ASCII );
         return reader;
      }

      public static BinaryWriter CreateWriter( Stream stream )
      {
         var writer = new BinaryWriter( stream, Encoding.ASCII );
         return writer;
      }

      public static void SerializeEntry<TEntry>( BinaryWriter writer, TEntry entry )
         where TEntry : IRedisEntry
      {
         writer.Write( entry.GetTypeCode() );
         writer.Write( entry.GetId() );
         writer.Write( entry.GetTimestamp().Ticks );
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TEntry>( BinaryReader reader )
         where TEntry : IRedisEntry
      {
         var typeCode = reader.ReadUInt16();
         var entry = (TEntry)TsdbTypeRegistry.CreateEntry( typeCode );
         entry.SetId( reader.ReadString() );
         entry.SetTimestamp( new DateTime( reader.ReadInt64(), DateTimeKind.Utc ) );
         entry.Read( reader );
         return entry;
      }

      public static List<byte[]> Serialize<TEntry>( List<TEntry> entries, int maxByteArraySize )
         where TEntry : IRedisEntry
      {
         var results = new List<byte[]>();
         var stream = new MemoryStream();
         var writer = CreateWriter( stream );

         int currentSize = 0;
         List<byte[]> serializedEntries = new List<byte[]>();

         for( int i = entries.Count - 1 ; i > -1 ; i-- )
         {
            var entry = entries[ i ];

            if( currentSize + TsdbTypeRegistry.MaxEntrySizeInBytes > maxByteArraySize )
            {
               // create big array from mall the small arrays
               var data = CreateData( currentSize, serializedEntries );

               // add the created result
               results.Add( data );

               // reset parameters
               currentSize = 0;
               serializedEntries = new List<byte[]>( serializedEntries.Count );
            }

            SerializeEntry( writer, entry );
            writer.Flush(); // is this needed for a memory stream????

            var serializedEntry = stream.ToArray();
            serializedEntries.Add( serializedEntry );

            // update parameters
            currentSize += serializedEntry.Length;

            // reset stream
            stream.Seek( 0, SeekOrigin.Begin );
            stream.SetLength( 0 );
         }

         if( currentSize > 0 )
         {
            var data = CreateData( currentSize, serializedEntries );

            results.Add( data );
         }

         writer.Dispose();

         return results;
      }

      private static byte[] CreateData( int size, List<byte[]> serializedEntries )
      {
         var data = new byte[ size ];
         var copied = 0;
         for( int j = serializedEntries.Count - 1 ; j > -1 ; j-- )
         {
            var serializedEntry = serializedEntries[ j ];
            Buffer.BlockCopy( serializedEntry, 0, data, copied, serializedEntry.Length );
            copied += serializedEntry.Length;
         }
         return data;
      }

      public static List<TEntry> Deserialize<TEntry>( byte[] bytes )
         where TEntry : IRedisEntry
      {
         var stream = new MemoryStream( bytes );
         var reader = CreateReader( stream );
         List<TEntry> entries = new List<TEntry>();

         while( stream.Length != stream.Position )
         {
            var entry = DeserializeEntry<TEntry>( reader );
            entries.Add( entry );
         }

         return entries;
      }
   }
}
