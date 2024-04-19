/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include <iostream>
#include <limits>
#include "sql/parser/value.h"
#include <iomanip>
#include <cstring>
#include <climits>
#include <vector>
#include <cfloat>

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  //trx_ = trx;
  //result_tuple_.set_cells(std::vector<Value>(aggregations_.size(), Value(static_cast<float>(0.0)))); // Initialize with zero values
  return RC::SUCCESS;
}
// RC AggregatePhysicalOperator::next() {
//   if (result_tuple_.cell_num() > 0) {
//     return RC::RECORD_EOF;
//   }

//   RC rc = RC::SUCCESS;
//   PhysicalOperator *oper = children_[0].get();
//   int opnumber = (int)aggregations_.size();
//   std::vector<Value> result_cells(opnumber);  // 初始化调整
//   std::vector<int> count_cells(opnumber, 0);

//   // 初始化MAX和MIN操作的result_cells
//   for (int i = 0; i < opnumber; i++) {
//     switch (aggregations_[i]) {
//       case AggrOp::AGGR_MAX:
//         result_cells[i].set_float(-FLT_MAX); // 设置为最小浮点数
//         break;
//       case AggrOp::AGGR_MIN:
//         result_cells[i].set_float(FLT_MAX); // 设置为最大浮点数
//         break;
//       default:
//         result_cells[i].set_float(0.0); // 其他情况默认初始化为0.0
//         break;
//     }
//   }

//   while (RC::SUCCESS == (rc = oper->next())) {
//     Tuple *tuple = oper->current_tuple();

//     for (int cell_idx = 0; cell_idx < opnumber; cell_idx++) {
//       const AggrOp aggregation = aggregations_[cell_idx];
//       Value cell;
//       rc = tuple->cell_at(cell_idx, cell);
//       if (rc != RC::SUCCESS) {
//         return rc;  // 如果无法访问单元格，则传播错误。
//       }

//       switch (aggregation) {
//         case AggrOp::AGGR_SUM:
//         case AggrOp::AGGR_AVG:
//           if (cell.attr_type() == AttrType::INTS) {
//             result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + static_cast<float>(cell.get_int()));
//           } else if (cell.attr_type() == AttrType::FLOATS) {
//             result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
//           }
//           if (aggregation == AggrOp::AGGR_AVG) {
//             count_cells[cell_idx]++;
//           }
//           break;
//         case AggrOp::AGGR_MAX:
//          if (cell.attr_type() == AttrType::CHARS) {
//          if (result_cells[cell_idx].get_string().compare(cell.get_string()) < 0) {
//          result_cells[cell_idx] = cell;
//         }
//          }else{
//           if (result_cells[cell_idx].get_float() < cell.get_float()) {
//             result_cells[cell_idx] = cell;
//           }
//          }
//           break;
//         case AggrOp::AGGR_MIN:
//         if (cell.attr_type() == AttrType::CHARS) {
//       if (result_cells[cell_idx].get_string().compare(cell.get_string()) > 0) {
//         result_cells[cell_idx] = cell;
//       }
//     } else {
//           if (result_cells[cell_idx].get_float() > cell.get_float()) {
//             result_cells[cell_idx] = cell;
//           }
//     }
//           break;
//         case AggrOp::AGGR_COUNT:
//         case AggrOp::AGGR_COUNT_ALL:
//           count_cells[cell_idx]++;
//           break;
//       }
//     }
//   }

//   // Finalize AVG computation and handle COUNT
//   for (int i = 0; i < opnumber; i++) {
//     if (aggregations_[i] == AggrOp::AGGR_AVG && count_cells[i] > 0) {
//       result_cells[i].set_float(result_cells[i].get_float() / count_cells[i]);
//     } 
//     else if (aggregations_[i] == AggrOp::AGGR_COUNT || aggregations_[i] == AggrOp::AGGR_COUNT_ALL) {
//       result_cells[i].set_int(count_cells[i]);
//     }
//   }

//   if (rc == RC::RECORD_EOF) {
//     rc = RC::SUCCESS;
//   }

//   result_tuple_.set_cells(result_cells);
//   return rc;
// }

RC AggregatePhysicalOperator::next()
{
  // already aggregated
  if (result_tuple_.cell_num() > 0) {
    return RC ::RECORD_EOF;
  }
  RC                 rc   = RC ::SUCCESS;
  PhysicalOperator  *oper = children_[0].get();
  int                opnumber = (int)aggregations_.size();
  std::vector<Value> result_cells(opnumber);
  int cell_flag = 0;
  
  // 先在此处对result_cells进行初始化，后续功能自行添加
  for(int i = 0; i < opnumber; i++) 
  {
    const AggrOp aggregation = aggregations_[i];
    if (aggregation == AggrOp::AGGR_SUM) 
    {
      result_cells[i].set_type(AttrType::FLOATS);
      result_cells[i].set_float(0.0);
    }
  }

  std::vector<Value> sum_cells(opnumber);
          for(int i = 0; i < opnumber; i++) 
          {
            const AggrOp aggregation_ = aggregations_[i];    
            if(aggregation_ == AggrOp::AGGR_AVG)   
            {
            sum_cells[i].set_type(AttrType::FLOATS);
            sum_cells[i].set_float(0.0);   
            }        
          }
  std::vector<int> count_cells(aggregations_.size(),0);

  std::vector<double> max_cells(aggregations_.size(),-std::numeric_limits<double>::infinity());
  std::vector<string> max_cells_(opnumber,"");

  std::vector<double> min_cells(aggregations_.size(),std::numeric_limits<double>::infinity());
  std::vector<string> min_cells_(opnumber,"");

  std::string result_string;

  int my_index = 0;
          
  while (RC ::SUCCESS == (rc = oper->next())) {
    // get tuple
    Tuple *tuple = oper->current_tuple();

    // do aggregate
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];
      Value        cell;
      AttrType     attr_type = AttrType::INTS;
      switch (aggregation) {
        case AggrOp::AGGR_SUM:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (attr_type == AttrType ::INTS or attr_type == AttrType ::FLOATS) {
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;

          case AggrOp::AGGR_AVG:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
         // cell_flag = 1;
          if (attr_type == AttrType ::INTS or attr_type == AttrType ::FLOATS)
          {
            sum_cells[cell_idx].set_float(sum_cells[cell_idx].get_float() + cell.get_float());  
            count_cells[cell_idx]++;  
            
          }
          break;

          case AggrOp::AGGR_MAX:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
        //  cell_flag = 2;
          if (attr_type == AttrType ::INTS or attr_type == AttrType ::FLOATS )
          {
            double cell_value = (attr_type == AttrType::INTS) ? static_cast<double>(cell.get_int()) : cell.get_float();
            if (cell_value > max_cells[cell_idx])
            {
              max_cells[cell_idx] = cell_value;
            }
          }
           else if (attr_type == AttrType::CHARS) {  
            cell_flag = 1;
            std::string cell_value = cell.get_string();  
            if (cell_value > max_cells_[cell_idx]) {  
                max_cells_[cell_idx] = cell_value;  
             }  
           }  
           else if(attr_type == AttrType::DATES){
            cell_flag = 2;
            int date_value = cell.get_int();
            if (date_value > max_cells[cell_idx]) {
                max_cells[cell_idx] = date_value;
            }
           }
          break;

          case AggrOp::AGGR_MIN:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          //cell_flag = 3;
          if (attr_type == AttrType ::INTS or attr_type == AttrType ::FLOATS )
          {
            double cell_value = (attr_type == AttrType::INTS) ? static_cast<double>(cell.get_int()) : cell.get_float();
            if (cell_value < min_cells[cell_idx])
            {
              min_cells[cell_idx] = cell_value;
            }
          }
           else if (attr_type == AttrType::CHARS) {  
            cell_flag = 1;
             std::string cell_value = cell.get_string();  
             if (my_index == 0)
             {
              min_cells_[my_index] = cell_value;  
             }
            if (cell_value < min_cells_[cell_idx] ) {  
                min_cells_[cell_idx] = cell_value;  
             }  
           } 
           else if(attr_type == AttrType::DATES){
            cell_flag = 2;
            int date_value = cell.get_int();
            if (date_value < min_cells[cell_idx]) {
                min_cells[cell_idx] = date_value;
            }
           }
          break;

          case AggrOp::AGGR_COUNT:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          //cell_flag = 4;
          count_cells[cell_idx]++;  
          break;

          case AggrOp::AGGR_COUNT_ALL:
          rc        = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          //cell_flag = 4;
          count_cells[cell_idx]++;  
          break;

        default: return RC::UNIMPLENMENT;
      }
    }
    my_index++;
  }
  //if(cell_flag == 1)
  {
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {  
                if (count_cells[cell_idx] > 0) 
                {  
                     result_cells[cell_idx].set_float(sum_cells[cell_idx].get_float() / count_cells[cell_idx]);  
                }
               }
  }
  //if (cell_flag == 2)
  {
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {  
    if (aggregations_[cell_idx] == AggrOp::AGGR_MAX) {  
        const char *sp_cell = max_cells_[cell_idx].c_str();
        if (cell_flag==1)
        {
          result_cells[cell_idx].set_string(sp_cell);  
        }
        else if(cell_flag==2){
          result_cells[cell_idx].set_date(max_cells[cell_idx]);  
        }
        else
        result_cells[cell_idx].set_float(max_cells[cell_idx]);  
       }  
   }
  }

  //if (cell_flag == 3)
  {
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {  
    if (aggregations_[cell_idx] == AggrOp::AGGR_MIN) {  
        const char *sp_cell = min_cells_[cell_idx].c_str();
        if (cell_flag==1)
        {
          result_cells[cell_idx].set_string(sp_cell);  
        }
        else if(cell_flag==2){
          result_cells[cell_idx].set_date(min_cells[cell_idx]);  
        }
        else
        result_cells[cell_idx].set_float(min_cells[cell_idx]);  
       }  
   }
  }

  //if (cell_flag == 4)
  {
    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {  
    if (aggregations_[cell_idx] == AggrOp::AGGR_COUNT or aggregations_[cell_idx] == AggrOp::AGGR_COUNT_ALL) {  
        result_cells[cell_idx].set_float(count_cells[cell_idx]);  
       }  
   }
  }

  if (rc == RC ::RECORD_EOF) {
    rc = RC ::SUCCESS;
  }
  result_tuple_.set_cells(result_cells);
  return rc;
}




RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation) {
    aggregations_.push_back(aggregation);
}

