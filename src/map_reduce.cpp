//---------------------------------------------------------------------------
#include <fstream> //  открываем, читаем файл
#include <string>
#include <sstream>
#include <iostream>
//---------------------------------------------------------------------------
#include "map_reduce.h"
//---------------------------------------------------------------------------
#include "sync_console.h"
//---------------------------------------------------------------------------


t_map_reduce::t_map_reduce(const std::filesystem::path& a_init_file, int an_num_of_mapper, int an_num_of_reducer, 
  t_func_mapper a_func_mapper, t_func_reduce a_func_reduce)
  : m_init_file(a_init_file),
    m_num_of_mappers(an_num_of_mapper),
    m_num_of_reducers(an_num_of_reducer),
    m_func_mapper(a_func_mapper),
    m_func_reduce(a_func_reduce)
{
}
//---------------------------------------------------------------------------


t_map_reduce::~t_map_reduce()
{
}
//---------------------------------------------------------------------------



// выполни работу
int t_map_reduce::do_work()
{

  // TODO: как некий "заранее дебаг" в нашей версии ещё можно добавить проверку на не повторение в строках. 
  // Иначе всё равно не найдём ничего. Но в общей случае в реальном map-reduce мы этого конечно знать не можем

  // узнаем максимальну длину строки исходного файла. Вероятно это конечно не по "map-reduce-ски", но
  // наверно для домашнего задания пойдёт. Если такая задача будет в реальности, то возможно стиот на
  // стадии map делать из каждой строки все записи и с префиком одна буква и 2 и 3, и так до конца. 
  // а потом уже после редюсеров собрать минимальный ответ, который ещё проходит
  size_t un_maxline_length = 0;
  {
    // откроем сразу, чтобы по 100 раз не открывать/ закрывать:
    std::ifstream file(m_init_file, std::ios::binary);

    std::string str_curr;
    while (std::getline(file, str_curr)) {

      // убираем лишний \r в конце строки (во всяком случае так под Windows)
      std::stringstream ss(str_curr);
      std::string trimmed_string;
      ss >> str_curr;

      if (un_maxline_length < str_curr.size()) {
        un_maxline_length = str_curr.size();

        // было для отладки:
        //clog::logout("new max string: " + str_curr);
      }
    };
  }

  clog::logout("max length of string in file: " + std::to_string(un_maxline_length));

  if (un_maxline_length == 0) {
    clog::log_err("max length is 0. empty file or wrong format?");
    return -33;
  }


  // теперь проходим циклом до тех пор пока не найдём, что всё получилось:
  bool f_is_ok = false;

  for(size_t i = 1; i <= un_maxline_length; i++) 
  {
    clog::logout("\r\n******** >> start test with prefix length: " + std::to_string(i));

    int n_res = do_work_one_iter(i, f_is_ok);
    if (n_res) {
      clog::log_err("Error in do_work_one_iter(). code: " + std::to_string(n_res));
      return n_res;
    }

    if (f_is_ok == false) {
      clog::logout("<< *** iteration was not succeded, prefix length: " + std::to_string(i));
      continue;
    }

    clog::logout("<< *** iteration succeded. Prefix length: " + std::to_string(i));
    break;
  }

  return 0;
}
//---------------------------------------------------------------------------


// рассчитываем проход при такой длине префикса
int t_map_reduce::do_work_one_iter(size_t aun_prefix_length, bool& af_is_ok)
{
  // "обнуляем" выходные данные:
  af_is_ok = false;


  // Разбиваем на блоки исходный файл:
  std::vector<std::pair<std::uintmax_t, uint64_t>> vec_blocks = calc_blocks();

  // мелкий дебаг, что правильно разбили:
  std::uintmax_t un_total = 0;
  for(auto& curr : vec_blocks)
  {
    clog::log_err("start: " + std::to_string(curr.first) + ", length: " + std::to_string(curr.second));
    un_total += curr.second;
  }

  clog::log_err("total from block size: " + std::to_string(un_total));


  // тип одного блока, чтобы покороче писать:
  using t_vec_one_block = std::vector<std::pair<std::string, std::string>>;


  std::vector<t_vec_one_block> vec_all_blocks(vec_blocks.size());


  // заготовим нужное число ниток:
  std::vector<std::thread> vec_threads(vec_blocks.size());
  for(size_t i=0; i<vec_blocks.size(); ++i) 
  {
    auto n_start = vec_blocks[i].first;
    auto n_length = vec_blocks[i].second;

    auto& vec_job = vec_all_blocks[i];

    vec_threads[i] = std::thread([this, n_start, n_length, aun_prefix_length, &vec_job] ()
    {
      int n_res = this->map(n_start, n_length, aun_prefix_length, vec_job);
      if (n_res) {
        clog::log_err("error in map(). code: " + std::to_string(n_res));
      }
      //clog::logout("some value in thread. n: " + std::to_string(n));
    });
  }


  clog::logout("before join() map threads");
  for (auto& curr : vec_threads) {
    curr.join();
  };
  clog::logout("after join() map threads");







  
  // По заданию просят создать "R контейнеров", которые потом пойдут в редюсеры
  // (хотя наверно можно было бы "пушить" задачи в них "налету")
  using t_vec_one_reduce_cntr = std::vector<std::pair<std::string, std::string>>;
  std::vector<t_vec_one_reduce_cntr> vec_reduce_cntrs(m_num_of_reducers);


  std::vector<t_rec> vec_positions;
  vec_positions.reserve(vec_all_blocks.size());


  // первый проход заполняем отдельно:
  for(size_t i=0; i<vec_all_blocks.size(); ++i)
  {
    if(!(vec_all_blocks[i].size())) {
      // не будем ничего добавлять - должно и так работать по идее
      continue;
    }
    
    vec_positions.emplace_back(i, 0);
  }


  // функция сортировки - нужна в двух местах. Вытащил сюда
  auto sort_func = [&vec_all_blocks](const t_rec& a, const t_rec& b) -> bool
  {
    const std::string& str1 = vec_all_blocks[a.m_block_index][a.m_position_in_block].first;
    const std::string& str2 = vec_all_blocks[b.m_block_index][b.m_position_in_block].first;

    return str1 > str2;
  };
  


  auto print_func = [&vec_all_blocks, &vec_positions]() {
    for (auto& curr : vec_positions) {
      clog::logout("  key: " + vec_all_blocks[curr.m_block_index][curr.m_position_in_block].first);
    }
  };



  //print_func();

  // сделаем из вектора heap, чтобы было просто вытаскивать элементы
  std::make_heap(vec_positions.begin(), vec_positions.end(), sort_func);


  //print_func();
  

  int n_num_of_iter = 0;
  while(true)
  {
    
    // сначала пытаемся вытащить самый максимальный:
    if (!(vec_positions.size())) {
      break;
    }
   
    std::pop_heap(vec_positions.begin(), vec_positions.end(), sort_func);
    
    const auto& max = vec_positions.back();
    
    vec_positions.pop_back();
    


    // Здесь полезная работа (в данной работе сначала просят всё запихнуть в контейнеры)
    {
      // считаем хеш от строки, и остаток от деления на число редюсеров
      size_t n_reduce_id = (std::hash<std::string>{}(vec_all_blocks[max.m_block_index][max.m_position_in_block].first)) % m_num_of_reducers;

      // запихиваем в нужный контейнер:
      vec_reduce_cntrs[n_reduce_id].emplace_back(vec_all_blocks[max.m_block_index][max.m_position_in_block].first, vec_all_blocks[max.m_block_index][max.m_position_in_block].second);

      n_num_of_iter++;
    }

    
    // Теперь запихиваем в хип следующий элемент из этого же блока:
    {
      size_t new_position = max.m_position_in_block + 1;
      if(vec_all_blocks[max.m_block_index].size() > new_position) {
        vec_positions.emplace_back(max.m_block_index, new_position);
      }
    }
  }

  clog::logout("Num of iterations passed: " + std::to_string(n_num_of_iter));




  // Запускаем нити, ждём завершения:
  // заготовим нужное число ниток:
  {
    // сюда сохраним результаты каждого редюсера:
    std::vector<bool> vec_results(m_num_of_reducers);


    clog::logout("going to start reducers...");

    std::vector<std::thread> vec_threads(m_num_of_reducers);
    for(size_t i=0; i<vec_threads.size(); ++i)
    {

      const auto& curr_cntr = vec_reduce_cntrs[i];

      vec_threads[i] = std::thread([this, &curr_cntr, i, &vec_results]()
      {
        // делаем себе собственную копию функтора в этой нити:
        auto local_functor = this->m_func_reduce;

        for(const auto& curr_item : curr_cntr)
        {
          /*bool f_result = this->m_vec_reducers[i]->process(curr_item.first, curr_item.second);
          if (!f_result) {
            vec_results[i] = false;
            return;
          }
          */

          bool f_result = local_functor(curr_item.first, curr_item.second);
          if (!f_result) {
            vec_results[i] = false;
            return;
          }

        }

        // Если дошли до сюда, значит всё прошло норм:
        vec_results[i] = true;

      });
    }

    clog::logout("before join() reduce threads");
    for (auto& curr : vec_threads) {
      curr.join();
    };
    clog::logout("after join() reduce threads");

    

    // Посмотрим что с результатами:
    for(auto curr : vec_results)
    {
      if (!curr) {
        clog::log_err("At least some of reducers returned 'false'");
        af_is_ok = false;

        return 0;
      }
    }
  }  

  af_is_ok = true;
  return 0;
}
//---------------------------------------------------------------------------



std::vector<std::pair<std::uintmax_t, uint64_t>> t_map_reduce::calc_blocks()
{

  std::error_code ec;
  std::uintmax_t n_file_size = std::filesystem::file_size(m_init_file, ec);
  if (ec) {
    clog::log_err("Error getting filesize of file: " + m_init_file.string());
    //return -33;
    throw std::runtime_error((std::string("Error getting filesize of file: ") + m_init_file.string()).c_str());
  }

  clog::logout("size of income file: " + std::to_string(n_file_size));


  // сюда сложим блоки уточнённые куски:
  std::vector<std::pair<std::uintmax_t, uint64_t>> vec_blocks;


  // если мапер один, то считать нечего:
  if (m_num_of_mappers == 1) {
    clog::log_err("just one mappers needed");
    vec_blocks.emplace_back(0, n_file_size);
    return vec_blocks;
  }



  std::uintmax_t n_block_size = n_file_size / m_num_of_mappers;
  clog::log_err("calculated approx size of block: " + std::to_string(n_block_size));


  {
    // откроем сразу, чтобы по 100 раз не открывать/ закрывать:
    std::ifstream file(m_init_file, std::ios::binary);

    std::uintmax_t n_pre_offset = 0;
    for(int i=0; i<m_num_of_mappers; i++)
    {
      std::uintmax_t n_curr_ptr = n_block_size * (i+1);

      // переехали в позицию:
      file.seekg(n_curr_ptr);


      char char_curr = 0;
      
      uint64_t extra_offset = 0;
      do
      {
        // вдруг уже дошли до конца файла. Тогда читать дальше нельзя:
        if (file.eof()) {
          break;
        }

        file.read(&char_curr, 1);
        if (!file.gcount()) {
          break;
        }
        extra_offset ++;
      } while(char_curr != '\n');

      vec_blocks.emplace_back(n_pre_offset, (n_curr_ptr + extra_offset) - n_pre_offset);

      n_pre_offset = n_curr_ptr + extra_offset;
    }
  }

  // Посмотрим что там с остатком:
  uint64_t un_last_position = (vec_blocks[vec_blocks.size() - 1]).first + (vec_blocks[vec_blocks.size() - 1]).second;
  if(n_file_size > un_last_position)
  {
    vec_blocks.emplace_back(un_last_position, n_file_size - un_last_position);
  }

  return vec_blocks;
}
//---------------------------------------------------------------------------



// чего-то куда-то мапируем:
int t_map_reduce::map(
  std::uintmax_t aun_start, uint64_t aun_size, size_t aun_prefix_len,
  std::vector<std::pair<std::string, std::string>>& avec_result)
{

  // откроем сразу, чтобы по 100 раз не открывать/ закрывать:
  std::ifstream file(m_init_file, std::ios::binary);

  file.seekg(aun_start);
  
  {
    std::string str_curr;
    while (std::getline(file, str_curr))
    {

      // убираем лишний \r в конце строки (во всяком случае так под Windows)
      std::stringstream ss(str_curr);
      std::string trimmed_string;
      ss >> str_curr;

      // всё в нижний регистр:
      std::transform(str_curr.begin(), str_curr.end(), str_curr.begin(),
        [](unsigned char c) { return std::tolower(c); });


      // дочитали до конца
      if (file.tellg() == -1) {
        break;
      }

      // если "заехали" за границу куска, то выходим. Это уже не наше
      auto pos = file.tellg();
      if (pos > aun_start + aun_size) {
        break;
      }

      if (!str_curr.length()) {
        clog::log_err("some empty string in file???. so skippin it");
        continue;
      }

      std::pair<std::string, std::string> res = m_func_mapper(str_curr, aun_prefix_len);
      avec_result.emplace_back(res);
      
    }
  }

  std::sort(avec_result.begin(), avec_result.end(),
    [](const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) -> bool
    {
      return a.first < b.first;
    });

  

  return 0;
}
//---------------------------------------------------------------------------