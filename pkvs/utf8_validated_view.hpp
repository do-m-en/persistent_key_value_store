#ifndef UTF8_VALIDATED_VIEW_HPP_INCLUDED
#define UTF8_VALIDATED_VIEW_HPP_INCLUDED

#include <bit>
#include <cassert>
#include <optional>
#include <string_view>

namespace pkvs
{
  namespace detail
  {
    // taken from https://stackoverflow.com/questions/28270310/how-to-easily-detect-utf8-encoding-in-the-string
    constexpr bool validate_utf8(const char* string) noexcept // TODO rewrite to accept std::string_view
    {
      assert(string != nullptr);

      while (*string)
      {
        switch (std::countl_one(static_cast<unsigned char>(*string)))
        {
          [[unlikely]] case 4: ++string; if (std::countl_one(static_cast<unsigned char>(*string)) != 1) return false; [[fallthrough]];
          [[unlikely]] case 3: ++string; if (std::countl_one(static_cast<unsigned char>(*string)) != 1) return false; [[fallthrough]];
          [[unlikely]] case 2: ++string; if (std::countl_one(static_cast<unsigned char>(*string)) != 1) return false; [[fallthrough]];
            [[likely]] case 0: ++string; break;
          [[unlikely]] default: return false;
        }
      }

      return true;
    }
  }

  class utf8_validated_view
  {
  public:
    static std::optional<utf8_validated_view> make( std::string_view view )
    {
      if( detail::validate_utf8( view.data() ) == false )
        return std::nullopt;

      return utf8_validated_view{ view };
    }

  private:
    utf8_validated_view( std::string_view view ) : view_{ view } {}

    std::string_view view_;
  };
}

#endif // UTF8_VALIDATED_VIEW_HPP_INCLUDED