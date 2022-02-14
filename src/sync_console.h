//---------------------------------------------------------------------------
#pragma once
//---------------------------------------------------------------------------
#include <string>
#include <string_view>
#include <mutex>
//---------------------------------------------------------------------------
class t_sync_console;
//---------------------------------------------------------------------------

// ссылка на единый лог
extern t_sync_console* gp_log;
//---------------------------------------------------------------------------



namespace clog
{
  void logout(const std::string_view& astr_info);
  void log_info(const std::string_view& astr_info);
  void log_err(const std::string_view& astr_info);

  // чтобы выводилось всегда - там по заданию нужно в консоль
  void log_info_always(const std::string_view& astr_info);
}



class t_sync_console
{
public:

  void logout_inst(const std::string_view& astr_info);

  void log_err_inst(const std::string_view& astr_info);



  void log_info_inst(const std::string_view& astr_info);

private:

  // мьютекс синхронизации std::cout
  mutable std::mutex m_mutex; // оставил один - иначе cout и cerr в одной консоли смешиваются
  //mutable std::mutex m_mut_cout;
  //mutable std::mutex m_mut_cerr;
};
//---------------------------------------------------------------------------