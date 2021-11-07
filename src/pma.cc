#include "pma.h"

namespace cobtree {

  PMASegment PMA::Get(uint64_t segment_id) const {
    /*not implemented*/
    char *ptr = storage_.get()->buffer_.get() + segment_id * segment_size_;
    return PMASegment{
      .content =  ptr,
      .len =  segment_size_,
    };
  }

  uint64_t PMA::Add(const char *item, uint64_t segment_id, PMAUpdateContext *ctx) {
    /*not implemented*/
    return 0;
  }

  uint64_t PMA::Add(const char *item, uint64_t segment_id, uint64_t pos, PMAUpdateContext *ctx) {
    PMASegment segment = Get(segment_id);
    memcpy(segment.content - segment_size_, )
    memcpy(segment.content, item, unit_size_);
    std::shared_ptr<SegmentInfo> info = segment_infos_.find(segment_id)->second;
    info->num_count++;
    return segment_id * segment_size_ + pos;
  }

  bool PMA::MustRebalance(uint64_t segment_id, std::shared_ptr<SegmentInfo> info) {
    double density = double(info->num_count) / double(segment_size_);
    std::pair<double, double> thresholds = GetTargentDensity(height_ - 1);
    return density <= thresholds.first && height_ > 1 || density >= thresholds.second;
  }

  void PMA::Rebalance(uint64_t segment_id, std::shared_ptr<SegmentInfo> info, PMAUpdateContext *ctx) {
    int depth = height_ - 2;
    int divisor = 2;
    long rangeFrom = segment_id;
    long rangeTo = segment_id;
    long count = info->num_count;
    double density = double(count) / double(segment_size_);
    std::pair<double, double>thresholds = GetTargentDensity(height_ - 1);

    while (depth >= 0) {
      long start = rangeFrom / divisor * divisor;
      if (start == rangeFrom) {
        for (long i = rangeTo + 1; i < rangeTo + divisor; i++) {
          count += segment_infos_.find(i)->second->num_count;
        }
      } else {
        for (long i = start; i < rangeFrom; i++) {
          count += segment_infos_.find(i)->second->num_count;
        }
      }
      rangeFrom = start;
      rangeTo = start + divisor - 1;
      density = double(count) / double(divisor*segment_size_);
      thresholds = GetTargentDensity(depth);
      if (density > thresholds.first && density < thresholds.second) break;
      depth--;
      divisor *= 2;
    }

    if (depth < 0) {
      if (density >= thresholds.second) {
        // re-allocate a block device
        auto device = new BlockDevice(2 * segment_count_ * segment_size_ * unit_size_);
        for (int i = segment_count_; i < segment_count_*2; i++) {
          segment_infos_.insert(std::make_pair<uint64_t, std::shared_ptr<SegmentInfo>>(i, std::shared_ptr<SegmentInfo>(new SegmentInfo(i, 0))));
        }
        expand(device, ctx);
      }
    }
  }

  void PMA::expand(BlockDevice* newDevice, PMAUpdateContext *ctx) {

  }

  std::pair<double, double> PMA::GetTargentDensity(int depth) {
    if (height_ == 1)
      return std::make_pair(this->option_.upper_density_base_upper, this->option_.lower_density_base_lower);
    double gap = option_.upper_density_base_lower - option_.lower_density_base_upper;
    return std::make_pair(
      this->option_.lower_density_base_upper - gap * depth / (height_ - 1),
      this->option_.upper_density_base_lower + gap * depth / (height_ - 1)
    );
  }

  void PMA::Rebalance(uint64_t segment_id, PMAUpdateContext *ctx) {
    /*not implemented*/
  }

  void PMA::Reallocate(PMAUpdateContext *ctx) {
    /*not implemented*/
  }

  double PMA::UpperDensityThreshold(int depth) {
    1 /
  }

  double PMA::LowerDensityThreshold(int depth) {

  }


}  // namespace cobtree