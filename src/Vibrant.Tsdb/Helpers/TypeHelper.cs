using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Helpers
{
   internal static class TypeHelper
   {
      private static Dictionary<Type, object> _defaultValues = new Dictionary<Type, object>();

      public static T GetDefaultValue<T>()
      {
         object value;
         if( !_defaultValues.TryGetValue( typeof( T ), out value ) )
         {
            // We want an Func<T> which returns the default.
            // Create that expression here.
            Expression<Func<T>> e = Expression.Lambda<Func<T>>(
                // The default value, always get what the *code* tells us.
                Expression.Default( typeof( T ) )
            );

            // Compile and return the value.
            value = e.Compile()();

            _defaultValues[ typeof( T ) ] = value;
         }
         return (T)value;
      }

      public static object GetDefaultValue( this Type type )
      {
         // Validate parameters.
         if( type == null ) throw new ArgumentNullException( "type" );

         object value;
         if( !_defaultValues.TryGetValue( type, out value ) )
         {
            // We want an Func<object> which returns the default.
            // Create that expression here.
            Expression<Func<object>> e = Expression.Lambda<Func<object>>(
                // Have to convert to object.
                Expression.Convert(
                    // The default value, always get what the *code* tells us.
                    Expression.Default( type ), typeof( object )
                )
            );

            // Compile and return the value.
            value = e.Compile()();

            _defaultValues[ type ] = value;
         }
         return value;
      }
   }
}
