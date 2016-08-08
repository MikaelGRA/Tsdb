using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Files
{
   public static class QSFile
   {
      [ThreadStatic]
      private static byte[] fileBuf = new byte[ 128 * 1024 ];

      private static void SimpleCopyFilePart( FileStream f, long from, long to, int length )
      {
         f.Position = from;
         f.Read( fileBuf, 0, length );
         f.Position = to;
         f.Write( fileBuf, 0, length );
      }

      public static void CopyFilePart( string file, long fromPos, long toPos, long length )
      {
         using( FileStream f = new FileStream( file, FileMode.Open, FileAccess.ReadWrite ) )
            CopyFilePart( f, fromPos, toPos, length );
      }

      public static void CopyFilePart( FileStream f, long fromPos, long toPos, long length )
      {
         lock( fileBuf )
         {
            int bufSize = fileBuf.Length;
            if( toPos > fromPos )
            {
               if( toPos + length > f.Length )
                  throw new ArgumentOutOfRangeException( "toPos + length", "Destination range is out of file." );

               long i_to = toPos + length - bufSize;
               long i_from = fromPos + length - bufSize;
               for( long i = length / (long)bufSize ; i > 0 ; --i, i_from -= bufSize, i_to -= bufSize )
                  SimpleCopyFilePart( f, i_from, i_to, bufSize );

               int leftover = (int)( length % (long)bufSize );
               if( leftover > 0 )
                  SimpleCopyFilePart( f, fromPos, toPos, leftover );

            }
            else
            {
               if( fromPos + length > f.Length )
                  throw new ArgumentOutOfRangeException( "fromPos + length", "Source range is out of file." );

               long i_to = toPos;
               long i_from = fromPos;
               for( long i = length / (long)bufSize ; i > 0 ; --i, i_from += bufSize, i_to += bufSize )
                  SimpleCopyFilePart( f, i_from, i_to, bufSize );

               int leftover = (int)( length % (long)bufSize );
               if( leftover > 0 )
                  SimpleCopyFilePart( f, i_from, i_to, leftover );
            }
         }
      }

      public static void DeleteFilePart( string file, long startPos, long length )
      {
         using( FileStream f = new FileStream( file, FileMode.Open, FileAccess.ReadWrite ) )
            DeleteFilePart( f, startPos, length );

      }

      public static void DeleteFilePart( FileStream f, long startPos, long length )
      {
         if( startPos + length > f.Length )
            throw new ArgumentOutOfRangeException( "startPos + length", "Remove range is out of file." );

         long endPos = startPos + length;
         CopyFilePart( f, endPos, startPos, f.Length - endPos );
         f.SetLength( f.Length - length );
      }

      public static void InsertFilePart( string file, long startPos, long length )
      {
         using( FileStream f = new FileStream( file, FileMode.Open, FileAccess.ReadWrite ) )
            InsertFilePart( f, startPos, length );
      }

      public static void InsertFilePart( FileStream f, long startPos, long length )
      {
         if( startPos > f.Length )
            throw new ArgumentOutOfRangeException( "startPos", "Insertion position is out of file." );

         long endPos = startPos + length;
         f.SetLength( f.Length + length );
         CopyFilePart( f, startPos, endPos, f.Length - endPos );
      }

      public static void FillFilePart( string file, long startPos, long length, byte fillByte )
      {
         using( FileStream f = new FileStream( file, FileMode.Open, FileAccess.Write ) )
            FillFilePart( f, startPos, length, fillByte );
      }

      public static void FillFilePart( FileStream f, long startPos, long length, byte fillByte )
      {
         if( startPos + length > f.Length )
            throw new ArgumentOutOfRangeException( "startPos + length", "Fill range is out of file." );

         f.Position = startPos;
         for( long i = 0 ; i < length ; ++i )
            f.WriteByte( fillByte );
      }
   }
}
