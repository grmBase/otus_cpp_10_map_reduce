//---------------------------------------------------------------------------
#pragma once
//---------------------------------------------------------------------------
#include <functional>
#include <string>
#include <vector>
#include <filesystem>
//---------------------------------------------------------------------------



// Вспомогательная структура для хранения индекса блока и смещения в нём
// для сортировке в хипе
struct t_rec
{

  t_rec(size_t aun_map_index, size_t aun_pos_in_block)
    : //m_str_value(astr_value),
      m_block_index(aun_map_index),
      m_position_in_block(aun_pos_in_block)
  {};

  //std::string m_str_value;
  size_t m_block_index = 0;
  size_t m_position_in_block = 0;
};
//---------------------------------------------------------------------------




// типы функций, чтобы писать покороче
using t_func_reduce = std::function<bool(const std::string&, const std::string&)>;
using t_func_mapper = std::function<std::pair<std::string, std::string>(const std::string&, size_t)>;


class t_map_reduce
{
  public:

    t_map_reduce(const std::filesystem::path& a_init_file, int an_num_of_mappers, int an_num_of_reducers,
      t_func_mapper a_func_mapper, t_func_reduce a_func_reduce);

    // выполни работу
    int do_work();

    ~t_map_reduce();


  private:

    // рассчитываем проход при такой длине префикса
    int do_work_one_iter(size_t aun_prefix_length, bool& af_is_ok);

    // считаем блоки, на которые разбили:
    std::vector<std::pair<std::uintmax_t, uint64_t>> calc_blocks();

    // выполняем мапирование для указанного куска "файла":
    //std::vector<std::pair<std::string, std::string>> map(
    //  std::uintmax_t aun_start, uint64_t aun_size, int an_prefix_len);
    int map(
      std::uintmax_t aun_start, uint64_t aun_size, size_t aun_prefix_len,
      std::vector<std::pair<std::string, std::string>>& avec_result);


    // ************ Данные ****************
    std::filesystem::path m_init_file;
    int m_num_of_mappers = {0};
    int m_num_of_reducers = {0};


    // то что будем вызывать при проходе map-а:
    t_func_mapper m_func_mapper;
   
    // то что будем вызывать при проходе reduce-а:
    t_func_reduce m_func_reduce;
};
//---------------------------------------------------------------------------