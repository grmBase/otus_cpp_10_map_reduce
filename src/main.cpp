//---------------------------------------------------------------------------
#include <iostream>
//---------------------------------------------------------------------------
#include "map_reduce.h"
//---------------------------------------------------------------------------
#include "sync_console.h"
//---------------------------------------------------------------------------



// Функтор маппера:
auto func_mapper = [](const std::string& str_curr, size_t aun_prefix_len) -> std::pair<std::string, std::string> {
  if (str_curr.length() < aun_prefix_len) {
    // тут получается что это нормальная ситуация что некоторые почты будут полностью "опубликованы"
    // ну или кидать исключения (в реальной такой задаче про "не публикацию всего email-а")
    return std::make_pair(str_curr, "");
  }
  else {
    return std::make_pair(str_curr.substr(0, aun_prefix_len), str_curr.substr(aun_prefix_len));
  }
};
//---------------------------------------------------------------------------


// Код клиентского функтора редюсера 
struct
{

  bool operator()(const std::string& astr_key, const std::string& astr_payload)
  {

    // Если совсем первый раз, то просто запоминаем:
    if (m_str_curr_key == "") {
      m_str_curr_key = astr_key;
      m_str_all_info = astr_payload;
      return true;
    }


    // если пришёл новый ключ, то это новая серия:
    if (m_str_curr_key != astr_key) {

      // пишем новое:
      m_str_curr_key = astr_key;
      m_str_all_info = astr_payload;

      return true;
    }

    // Если идёт повтор, то это уже ошибка:
    clog::logout("double detected. key: " + astr_key +
      ", pre_paload: " + m_str_all_info + ", current payload: " + astr_payload);
    return false;
  }


  // Блок, который обрабатываем сейчас
  std::string m_str_curr_key;

  // собранная "нагрузка" с предыдущего шага - ради отладки где были дубликаты
  std::string m_str_all_info;

} func_reduce_instance;
//---------------------------------------------------------------------------



int main(int argc, char* argv[])
{
  std::cout << "in main() start" << std::endl;

  if (argc != 4) {
    std::cerr << "Usage: utility <path_to_file_with_emails> <map_num> <reduce_num>" << std::endl;
    return 1;
  }


  const std::string c_str_path = argv[1];
  int n_num_maps = std::atoi(argv[2]);
  int n_num_reduce = std::atoi(argv[3]);

  if (n_num_maps < 1 || n_num_reduce < 1) {
    std::cerr << "num of mappers or reducers can't be less than 1. Exiting" << std::endl;
    return 2;
  }

  std::cout << "Detected params: file: " << c_str_path 
            << ", num of mappers: " << n_num_maps 
            << ", num of reducers: " << n_num_reduce << std::endl;

  
  try
  {
    std::filesystem::path path(c_str_path);

    t_map_reduce map_reduce(path, n_num_maps, n_num_reduce, func_mapper, func_reduce_instance);

    int n_res = map_reduce.do_work();
    if(n_res) {
      clog::log_err("error in do_work()");
      return n_res;
    }
    
    clog::logout("do_work() completed OK");
  }
  catch (const std::exception& aexc)
  {
    clog::log_err(std::string("caught exeption: ") + aexc.what());
    return -33;
  }

  return 0;
}
//---------------------------------------------------------------------------