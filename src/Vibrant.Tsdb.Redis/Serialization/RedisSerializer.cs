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
      private static readonly int MaxEntrySizeInBytes = 1024;

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
         writer.Write( entry.GetTimestamp().Ticks );
         entry.Write( writer );
      }

      public static TEntry DeserializeEntry<TEntry>( string id, BinaryReader reader )
         where TEntry : IRedisEntry, new()
      {
         var entry = new TEntry();
         entry.SetId( id );
         entry.SetTimestamp( new DateTime( reader.ReadInt64(), DateTimeKind.Utc ) );
         entry.Read( reader );
         return entry;
      }

      public static List<byte[]> Serialize<TEntry>( string id, List<TEntry> entries, int maxByteArraySize )
         where TEntry : IRedisEntry
      {
         var results = new List<byte[]>();
         var stream = new MemoryStream();
         var writer = CreateWriter( stream );

         writer.Write( id );
         writer.Flush();
         int currentSize = 0;
         List<byte[]> serializedEntries = new List<byte[]>();
         byte[] idBytes = stream.ToArray();
         stream.Seek( 0, SeekOrigin.Begin );
         stream.SetLength( 0 );

         for( int i = entries.Count - 1 ; i > -1 ; i-- )
         {
            var entry = entries[ i ];

            if( currentSize + MaxEntrySizeInBytes > maxByteArraySize )
            {
               // create big array from mall the small arrays
               var data = CreateData( currentSize, idBytes, serializedEntries );

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
            var data = CreateData( currentSize, idBytes, serializedEntries );

            results.Add( data );
         }

         writer.Dispose();

         return results;
      }

      public static byte[] Serialize<TEntry>( string id, TEntry entry )
         where TEntry : IRedisEntry
      {
         var stream = new MemoryStream();
         using( var writer = CreateWriter( stream ) )
         {
            writer.Write( id );
            SerializeEntry( writer, entry );
            writer.Flush();
            return stream.ToArray();
         }
      }

      private static byte[] CreateData( int size, byte[] idBytes, List<byte[]> serializedEntries )
      {
         var data = new byte[ size + idBytes.Length ];
         Buffer.BlockCopy( idBytes, 0, data, 0, idBytes.Length );
         var copied = idBytes.Length;
         for( int j = serializedEntries.Count - 1 ; j > -1 ; j-- )
         {
            var serializedEntry = serializedEntries[ j ];
            Buffer.BlockCopy( serializedEntry, 0, data, copied, serializedEntry.Length );
            copied += serializedEntry.Length;
         }
         return data;
      }

      public static List<TEntry> Deserialize<TEntry>( byte[] bytes )
         where TEntry : IRedisEntry, new()
      {
         var stream = new MemoryStream( bytes );
         var reader = CreateReader( stream );
         List<TEntry> entries = new List<TEntry>();

         var id = reader.ReadString();

         while( stream.Length != stream.Position )
         {
            var entry = DeserializeEntry<TEntry>( id, reader );
            entries.Add( entry );
         }

         return entries;
      }
   }
}
