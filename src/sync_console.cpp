//---------------------------------------------------------------------------
#include <iostream>
#include <thread> //this_thread
//---------------------------------------------------------------------------
#include "sync_console.h"
//---------------------------------------------------------------------------



// пока включим отладку:
#define DBG_LOGGING

void t_sync_console::log_info_inst(const std::string_view& astr_info)
{
  std::lock_guard<std::mutex> lock(m_mutex);

  std::cout << astr_info << " [tid:" << std::this_thread::get_id() << "]" << std::endl;
}
//---------------------------------------------------------------------------


void t_sync_console::logout_inst(const std::string_view& astr_info)
{
#ifdef DBG_LOGGING
  log_info_inst(astr_info);
#endif
}
//---------------------------------------------------------------------------


// 
void t_sync_console::log_err_inst(const std::string_view& astr_info)
{

#ifdef DBG_LOGGING
  std::lock_guard<std::mutex> lock(m_mutex);

  std::cerr << astr_info << " [tid:" << std::this_thread::get_id() << "]" << std::endl;
#endif
}
//---------------------------------------------------------------------------



void clog::logout(const std::string_view& astr_info)
{
#ifdef DBG_LOGGING
  gp_log->logout_inst(astr_info);
#endif
}
//---------------------------------------------------------------------------

void clog::log_info(const std::string_view& astr_info)
{
#ifdef DBG_LOGGING
  gp_log->log_info_inst(astr_info);
#endif
}
//---------------------------------------------------------------------------

void clog::log_err(const std::string_view& astr_info)
{
#ifdef DBG_LOGGING
  gp_log->log_err_inst(astr_info);
#endif
}
//---------------------------------------------------------------------------


// чтобы выводилось всегда - там по заданию нужно в консоль
void clog::log_info_always(const std::string_view& astr_info)
{
  gp_log->log_info_inst(astr_info);
}
//---------------------------------------------------------------------------
