#include "utf8_validated_view.hpp"
#include <cassert>

int main()
{
  char const* ascii = "abc";
  char8_t const* utf8 = u8"abc";
  char16_t const* utf16 = u"abc";
  char32_t const* utf32 = U"abc";

  assert( pkvs::utf8_validated_view::make( std::string_view{ ascii } ) != std::nullopt );
  assert( pkvs::utf8_validated_view::make( std::string_view{ (char const*)utf8 } ) != std::nullopt );
  // FIXME doesn't detect wrong encodings
  //assert( pkvs::utf8_validated_view::make( std::string_view{ (char const*)utf16 } ) == std::nullopt );
  //assert( pkvs::utf8_validated_view::make( std::string_view{ (char const*)utf32 } ) == std::nullopt );
}