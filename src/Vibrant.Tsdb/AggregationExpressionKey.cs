using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class AggregationExpressionKey
   {
      private Type[] _types;
      private GroupMethod _method;

      public AggregationExpressionKey( GroupMethod method, Type[] types )
      {
         _method = method;
         _types = types;
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         var other = obj as AggregationExpressionKey;
         if( other == null )
         {
            return false;
         }

         var ok = _method == other._method && _types.Length == other._types.Length;
         if( ok )
         {
            var len = _types.Length;
            for( int i = 0 ; i < len ; i++ )
            {
               var t1 = _types[ i ];
               var t2 = other._types[ i ];
               if( t1 != t2 )
               {
                  return false;
               }
            }
            return true;
         }
         return false;
      }

      // override object.GetHashCode
      public override int GetHashCode()
      {
         int hash = (int)_method + _types.Length;

         for( int i = 0 ; i < _types.Length ; i++ )
         {
            hash += _types[ i ].GetHashCode() * ( i + 1 );
         }

         return hash;
      }
   }
}
