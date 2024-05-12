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

RC AggregatePhysicalOperator::next()
{
  // already aggregated
  if (result_tuple_.cell_num() > 0) {
    LOG_TRACE("already aggregated and return");
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  int count = 0;
  std::vector<Value> result_cells;
  // collect filtered tuples
  while (RC::SUCCESS == (rc = oper->next())) {
    // get tuple
    Tuple *tuple = oper->current_tuple();
    LOG_TRACE("got tuple: %s", tuple->to_string().c_str());

    // do aggregate
    for (int cell_idx = 0; cell_idx < aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];

      Value cell;
      AttrType attr_type = AttrType::INTS;
      switch (aggregation) {
        case AggrOp::AGGR_COUNT:
        case AggrOp::AGGR_COUNT_ALL:
          if (count == 0) {
            result_cells.push_back(Value(0));
            LOG_TRACE("init count. count=0");
          }
          result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
          LOG_TRACE("update count. count=%s", result_cells[cell_idx].to_string().c_str());
          break;
        case AggrOp::AGGR_MAX:
          rc = tuple->cell_at(cell_idx, cell);
          if (count == 0) {//检查是否是第一次进行最大值操作
            result_cells.push_back(cell);
            LOG_TRACE("init max. max=%s", result_cells[cell_idx].to_string().c_str());
          } else if (cell.compare(result_cells[cell_idx]) > 0) {//对比当前单元格值 cell 与已有的最大值：
            result_cells[cell_idx] = cell;
            LOG_TRACE("update max. max=%s", result_cells[cell_idx].to_string().c_str());
          }
          break;
        case AggrOp::AGGR_MIN:
          rc = tuple->cell_at(cell_idx, cell);
          if (count == 0) {
            result_cells.push_back(cell);
            LOG_TRACE("init min. min=%s", result_cells[cell_idx].to_string().c_str());
          } else if (cell.compare(result_cells[cell_idx]) < 0) {
            result_cells[cell_idx] = cell;
            LOG_TRACE("update min. min=%s", result_cells[cell_idx].to_string().c_str());
          }
          break;
        case AggrOp::AGGR_SUM:
        case AggrOp::AGGR_AVG:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (count == 0) {
            result_cells.push_back(Value(0.0f));
            LOG_TRACE("init sum/avg. sum=%s", result_cells[cell_idx].to_string().c_str());
          }
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
            LOG_TRACE("update sum/avg. sum=%s", result_cells[cell_idx].to_string().c_str());
          }
          break;
        default:
          LOG_WARN("unimplemented aggregation");
          return RC::UNIMPLENMENT;
      }
    }

    count++;
  }
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  // update avg
  for (int cell_idx = 0; cell_idx < result_cells.size(); cell_idx++) {
    const AggrOp aggr = aggregations_[cell_idx];
    if (aggr == AggrOp::AGGR_AVG) {
      result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() / count);
      LOG_TRACE("update avg. avg=%s", result_cells[cell_idx].to_string().c_str());
    }
  }
  result_tuple_.set_cells(result_cells);
  LOG_TRACE("save aggregation results");

  LOG_TRACE("aggregate rc=%d", rc);
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

