using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Helpers
{
   public static class GuidHelper
   {
      public static string ToShortString( this Guid guid )
      {
         string base64 = Convert.ToBase64String( guid.ToByteArray() );
         return Encode( base64 );
      }

      public static Guid ParseShortGuid( string guid )
      {
         var base64 = Decode( guid );
         var bytes = Convert.FromBase64String( base64 );
         return new Guid( bytes );
      }

      private static string Encode( string base64 )
      {
         var len = base64.Length - 2;
         var builder = new StringBuilder( len );
         for( int i = 0 ; i < len ; i++ )
         {
            var ch = base64[ i ];
            switch( ch )
            {
               case '/':
                  builder.Append( '_' );
                  break;
               case '+':
                  builder.Append( '-' );
                  break;
               default:
                  builder.Append( ch );
                  break;
            }
         }
         return builder.ToString();
      }

      private static string Decode( string encoded )
      {
         var len = encoded.Length;
         var builder = new StringBuilder( len );
         for( int i = 0 ; i < len ; i++ )
         {
            var ch = encoded[ i ];
            switch( ch )
            {
               case '_':
                  builder.Append( '/' );
                  break;
               case '-':
                  builder.Append( '+' );
                  break;
               default:
                  builder.Append( ch );
                  break;
            }
         }
         return builder.Append( "==" ).ToString();
      }
   }
}
