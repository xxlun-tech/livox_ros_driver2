//
// The MIT License (MIT)
//
// Copyright (c) 2022 Livox. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include "lddc.h"

#include "comm/comm.h"
#include "comm/ldq.h"
#include "driver_node.h"
#include "include/ros_headers.h"
#include "lds_lidar.h"

#include <iostream>

namespace livox_ros
{

/** Lidar Data Distribute Control--------------------------------------------*/
Lddc::Lddc(int format, int multi_topic, double frq, const std::string & frame_id)
: transfer_format_(format),
  use_multi_topic_(multi_topic),
  publish_frq_(frq),
  frame_id_(frame_id)
{
  publish_period_ns_ = kNsPerSecond / publish_frq_;
  lds_ = nullptr;
}

Lddc::~Lddc()
{
  PrepareExit();
  std::cout << "lddc destory!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
}

int Lddc::RegisterLds(Lds * lds)
{
  if (lds_ == nullptr)
  {
    lds_ = lds;
    return 0;
  }
  return -1;
}

void Lddc::DistributePointCloudData(void)
{
  if (!lds_)
  {
    std::cout << "lds is not registered" << std::endl;
    return;
  }
  if (lds_->IsRequestExit())
  {
    std::cout << "DistributePointCloudData is RequestExit" << std::endl;
    return;
  }

  lds_->pcd_semaphore_.Wait();
  for (uint32_t i = 0; i < lds_->lidar_count_; i++)
  {
    uint32_t lidar_id = i;
    LidarDevice * lidar = &lds_->lidars_[lidar_id];
    LidarDataQueue * p_queue = &lidar->data;
    if ((kConnectStateSampling != lidar->connect_state) || (p_queue == nullptr))
    {
      continue;
    }
    PollingLidarPointCloudData(lidar_id, lidar);
  }
}

void Lddc::DistributeImuData(void)
{
  if (!lds_)
  {
    std::cout << "lds is not registered" << std::endl;
    return;
  }

  if (lds_->IsRequestExit())
  {
    std::cout << "DistributeImuData is RequestExit" << std::endl;
    return;
  }

  lds_->imu_semaphore_.Wait();
  for (uint32_t i = 0; i < lds_->lidar_count_; i++)
  {
    uint32_t lidar_id = i;
    LidarDevice * lidar = &lds_->lidars_[lidar_id];
    LidarImuDataQueue * p_queue = &lidar->imu_data;
    if ((kConnectStateSampling != lidar->connect_state) || (p_queue == nullptr))
    {
      continue;
    }
    PollingLidarImuData(lidar_id, lidar);
  }
}

void Lddc::PollingLidarPointCloudData(uint8_t index, LidarDevice * lidar)
{
  LidarDataQueue * p_queue = &lidar->data;
  if (p_queue == nullptr || p_queue->storage_packet == nullptr) return;

  while (!lds_->IsRequestExit() && !QueueIsEmpty(p_queue))
  {
    if (kPointCloud2Msg == transfer_format_)
    {
      PublishPointcloud2(p_queue, index);
    }
    if (kAutowarePointCloud2Msg == transfer_format_)
    {
      PublishAutowarePointcloud2(p_queue, index);
    }
    else if (kLivoxCustomMsg == transfer_format_)
    {
      PublishCustomPointcloud(p_queue, index);
    }
  }
}

void Lddc::PollingLidarImuData(uint8_t index, LidarDevice * lidar)
{
  LidarImuDataQueue & p_queue = lidar->imu_data;
  while (!lds_->IsRequestExit() && !p_queue.Empty())
  {
    PublishImuData(p_queue, index);
  }
}

void Lddc::PrepareExit(void)
{
  if (lds_)
  {
    lds_->PrepareExit();
    lds_ = nullptr;
  }
}

void Lddc::PublishPointcloud2(LidarDataQueue * queue, uint8_t index)
{
  while (!QueueIsEmpty(queue))
  {
    StoragePacket pkg;
    QueuePop(queue, &pkg);
    if (pkg.points.empty())
    {
      printf("Publish point cloud2 failed, the pkg points is empty.\n");
      continue;
    }

    PointCloud2 cloud;
    InitPointcloud2Msg(pkg, cloud);
    PublishPointcloud2Data(index, cloud);
  }
}

void Lddc::PublishAutowarePointcloud2(LidarDataQueue * queue, uint8_t index)
{
  while (!QueueIsEmpty(queue))
  {
    StoragePacket pkg;
    QueuePop(queue, &pkg);
    if (pkg.points.empty())
    {
      printf("Publish point cloud2 failed, the pkg points is empty.\n");
      continue;
    }

    PointCloud2 cloud;
    InitAutowarePointcloud2Msg(pkg, cloud);
    PublishPointcloud2Data(index, cloud);
  }
}

void Lddc::PublishCustomPointcloud(LidarDataQueue * queue, uint8_t index)
{
  while (!QueueIsEmpty(queue))
  {
    StoragePacket pkg;
    QueuePop(queue, &pkg);
    if (pkg.points.empty())
    {
      printf("Publish custom point cloud failed, the pkg points is empty.\n");
      continue;
    }

    CustomMsg livox_msg;
    InitCustomMsg(livox_msg, pkg, index);
    FillPointsToCustomMsg(livox_msg, pkg);
    PublishCustomPointData(livox_msg, index);
  }
}

void Lddc::InitPointcloud2MsgHeader(PointCloud2 & cloud)
{
  cloud.header.frame_id.assign(frame_id_);
  cloud.height = 1;
  cloud.width = 0;
  cloud.fields.resize(7);
  cloud.fields[0].offset = 0;
  cloud.fields[0].name = "x";
  cloud.fields[0].count = 1;
  cloud.fields[0].datatype = PointField::FLOAT32;
  cloud.fields[1].offset = 4;
  cloud.fields[1].name = "y";
  cloud.fields[1].count = 1;
  cloud.fields[1].datatype = PointField::FLOAT32;
  cloud.fields[2].offset = 8;
  cloud.fields[2].name = "z";
  cloud.fields[2].count = 1;
  cloud.fields[2].datatype = PointField::FLOAT32;
  cloud.fields[3].offset = 12;
  cloud.fields[3].name = "intensity";
  cloud.fields[3].count = 1;
  cloud.fields[3].datatype = PointField::FLOAT32;
  cloud.fields[4].offset = 16;
  cloud.fields[4].name = "tag";
  cloud.fields[4].count = 1;
  cloud.fields[4].datatype = PointField::UINT8;
  cloud.fields[5].offset = 17;
  cloud.fields[5].name = "line";
  cloud.fields[5].count = 1;
  cloud.fields[5].datatype = PointField::UINT8;
  cloud.fields[6].offset = 18;
  cloud.fields[6].name = "timestamp";
  cloud.fields[6].count = 1;
  cloud.fields[6].datatype = PointField::FLOAT64;
  cloud.point_step = sizeof(LivoxPointXyzrtlt);
}

void Lddc::InitPointcloud2Msg(const StoragePacket & pkg, PointCloud2 & cloud)
{
  InitPointcloud2MsgHeader(cloud);

  cloud.width = pkg.points_num;
  cloud.row_step = cloud.width * cloud.point_step;

  cloud.is_bigendian = false;
  cloud.is_dense = true;

  cloud.header.stamp = rclcpp::Time(pkg.base_time);

  std::vector<LivoxPointXyzrtlt> points;
  points.reserve(pkg.points_num);
  for (size_t i = 0; i < pkg.points_num; ++i)
  {
    LivoxPointXyzrtlt point;
    point.x = pkg.points[i].x;
    point.y = pkg.points[i].y;
    point.z = pkg.points[i].z;
    point.reflectivity = pkg.points[i].intensity;
    point.tag = pkg.points[i].tag;
    point.line = pkg.points[i].line;
    point.timestamp = static_cast<double>(pkg.points[i].offset_time);
    points.push_back(std::move(point));
  }
  cloud.data.resize(pkg.points_num * sizeof(LivoxPointXyzrtlt));
  memcpy(cloud.data.data(), points.data(), pkg.points_num * sizeof(LivoxPointXyzrtlt));
}

void Lddc::InitAutowarePointcloud2MsgHeader(PointCloud2 & cloud)
{
  cloud.header.frame_id.assign(frame_id_);
  cloud.height = 1;
  cloud.width = 0;
  
  cloud.fields.resize(10);  // There are 10 fields in PointXYZIRCAEDT
  
  // Initialize each field with its appropriate offset, name, count, and datatype
  cloud.fields[0].offset = 0;
  cloud.fields[0].name = "x";
  cloud.fields[0].count = 1;
  cloud.fields[0].datatype = PointField::FLOAT32;
  
  cloud.fields[1].offset = 4;
  cloud.fields[1].name = "y";
  cloud.fields[1].count = 1;
  cloud.fields[1].datatype = PointField::FLOAT32;
  
  cloud.fields[2].offset = 8;
  cloud.fields[2].name = "z";
  cloud.fields[2].count = 1;
  cloud.fields[2].datatype = PointField::FLOAT32;

  cloud.fields[3].offset = 12;
  cloud.fields[3].name = "intensity";
  cloud.fields[3].count = 1;
  cloud.fields[3].datatype = PointField::UINT8;
  
  cloud.fields[4].offset = 13;
  cloud.fields[4].name = "return_type";
  cloud.fields[4].count = 1;
  cloud.fields[4].datatype = PointField::UINT8;
  
  cloud.fields[5].offset = 14;
  cloud.fields[5].name = "channel";
  cloud.fields[5].count = 1;
  cloud.fields[5].datatype = PointField::UINT16;
  
  cloud.fields[6].offset = 16;
  cloud.fields[6].name = "azimuth";
  cloud.fields[6].count = 1;
  cloud.fields[6].datatype = PointField::FLOAT32;
  
  cloud.fields[7].offset = 20;
  cloud.fields[7].name = "elevation";
  cloud.fields[7].count = 1;
  cloud.fields[7].datatype = PointField::FLOAT32;
  
  cloud.fields[8].offset = 24;
  cloud.fields[8].name = "distance";
  cloud.fields[8].count = 1;
  cloud.fields[8].datatype = PointField::FLOAT32;
  
  cloud.fields[9].offset = 28;
  cloud.fields[9].name = "time_stamp";
  cloud.fields[9].count = 1;
  cloud.fields[9].datatype = PointField::UINT32;

  cloud.point_step = sizeof(PointXYZIRCAEDT);
}

void Lddc::InitAutowarePointcloud2Msg(const StoragePacket & pkg, PointCloud2 & cloud)
{
  InitAutowarePointcloud2MsgHeader(cloud);

  cloud.width = pkg.points_num;
  cloud.row_step = cloud.width * cloud.point_step;

  cloud.is_bigendian = false;
  cloud.is_dense = true;

  cloud.header.stamp = rclcpp::Time(pkg.base_time);

  std::vector<PointXYZIRCAEDT> points;
  points.reserve(pkg.points_num);
  for (size_t i = 0; i < pkg.points_num; ++i)
  {
    PointXYZIRCAEDT point;
    point.x = pkg.points[i].x;
    point.y = pkg.points[i].y;
    point.z = pkg.points[i].z;
    point.intensity = pkg.points[i].intensity;
    point.return_type = 3;
    point.channel = pkg.points[i].line;
    point.azimuth = std::atan2(point.y, point.x);
    float xyDistance = std::hypot(point.x, point.y);
    point.elevation = std::atan2(point.z, xyDistance);
    point.distance = std::hypot(xyDistance, point.z);
    point.time_stamp = static_cast<uint32_t>(pkg.points[i].offset_time - pkg.base_time);
    points.push_back(std::move(point));
  }
  cloud.data.resize(pkg.points_num * sizeof(PointXYZIRCAEDT));
  memcpy(cloud.data.data(), points.data(), pkg.points_num * sizeof(PointXYZIRCAEDT));
}

void Lddc::PublishPointcloud2Data(
  const uint8_t index, const PointCloud2 & cloud)
{
  Publisher<PointCloud2>::SharedPtr publisher_ptr =
    std::dynamic_pointer_cast<Publisher<PointCloud2>>(GetCurrentPublisher(index));
  publisher_ptr->publish(cloud);
}

void Lddc::InitCustomMsg(CustomMsg & livox_msg, const StoragePacket & pkg, uint8_t index)
{
  livox_msg.header.frame_id.assign(frame_id_);
  livox_msg.header.stamp = rclcpp::Time(pkg.base_time);
  livox_msg.timebase = pkg.base_time;

  livox_msg.point_num = pkg.points_num;
  if (lds_->lidars_[index].lidar_type == kLivoxLidarType)
  {
    livox_msg.lidar_id = lds_->lidars_[index].handle;
  }
  else
  {
    printf("Init custom msg lidar id failed, the index:%u.\n", index);
    livox_msg.lidar_id = 0;
  }
}

void Lddc::FillPointsToCustomMsg(CustomMsg & livox_msg, const StoragePacket & pkg)
{
  uint32_t points_num = pkg.points_num;
  const std::vector<PointXyzlt> & points = pkg.points;
  for (uint32_t i = 0; i < points_num; ++i)
  {
    CustomPoint point;
    point.x = points[i].x;
    point.y = points[i].y;
    point.z = points[i].z;
    point.reflectivity = points[i].intensity;
    point.tag = points[i].tag;
    point.line = points[i].line;
    point.offset_time = static_cast<uint32_t>(points[i].offset_time - pkg.base_time);

    livox_msg.points.push_back(std::move(point));
  }
}

void Lddc::PublishCustomPointData(const CustomMsg & livox_msg, const uint8_t index)
{
  Publisher<CustomMsg>::SharedPtr publisher_ptr =
    std::dynamic_pointer_cast<Publisher<CustomMsg>>(GetCurrentPublisher(index));
  publisher_ptr->publish(livox_msg);
}

void Lddc::InitImuMsg(const ImuData & imu_data, ImuMsg & imu_msg)
{
  imu_msg.header.frame_id = frame_id_;
  imu_msg.header.stamp = rclcpp::Time(imu_data.time_stamp);  // to ros time stamp

  imu_msg.angular_velocity.x = imu_data.gyro_x;
  imu_msg.angular_velocity.y = imu_data.gyro_y;
  imu_msg.angular_velocity.z = imu_data.gyro_z;
  imu_msg.linear_acceleration.x = imu_data.acc_x;
  imu_msg.linear_acceleration.y = imu_data.acc_y;
  imu_msg.linear_acceleration.z = imu_data.acc_z;
}

void Lddc::PublishImuData(LidarImuDataQueue & imu_data_queue, const uint8_t index)
{
  ImuData imu_data;
  if (!imu_data_queue.Pop(imu_data)) return;

  ImuMsg imu_msg;
  InitImuMsg(imu_data, imu_msg);

  Publisher<ImuMsg>::SharedPtr publisher_ptr =
    std::dynamic_pointer_cast<Publisher<ImuMsg>>(GetCurrentImuPublisher(index));
  publisher_ptr->publish(imu_msg);
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::CreatePublisher(
  uint8_t msg_type, const std::string & topic_name, uint32_t queue_size)
{
  if (kPointCloud2Msg == msg_type)
  {
    DRIVER_INFO(*cur_node_, "%s publish use PointCloud2 format", topic_name.c_str());
    return cur_node_->create_publisher<PointCloud2>(topic_name, queue_size);
  }

  if (kAutowarePointCloud2Msg == msg_type)
  {
    DRIVER_INFO(*cur_node_, "%s publish use Autoware PointCloud2 format", topic_name.c_str());
    return cur_node_->create_publisher<PointCloud2>(topic_name, queue_size);
  }

  if (kLivoxCustomMsg == msg_type)
  {
    DRIVER_INFO(*cur_node_, "%s publish use livox custom format", topic_name.c_str());
    return cur_node_->create_publisher<CustomMsg>(topic_name, queue_size);
  }

  if (kLivoxImuMsg == msg_type)
  {
    DRIVER_INFO(*cur_node_, "%s publish use imu format", topic_name.c_str());
    return cur_node_->create_publisher<ImuMsg>(topic_name, queue_size);
  }

  PublisherPtr null_publisher(nullptr);
  return null_publisher;
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::GetCurrentPublisher(uint8_t handle)
{
  uint32_t queue_size = kMinEthPacketQueueSize;
  if (use_multi_topic_)
  {
    if (!private_pub_[handle])
    {
      char name_str[48];
      memset(name_str, 0, sizeof(name_str));

      std::string ip_string = IpNumToString(lds_->lidars_[handle].handle);
      snprintf(
        name_str, sizeof(name_str), "livox/lidar_%s", ReplacePeriodByUnderline(ip_string).c_str());
      std::string topic_name(name_str);
      queue_size = queue_size * 2;  // queue size is 64 for only one lidar
      private_pub_[handle] = CreatePublisher(transfer_format_, topic_name, queue_size);
    }
    return private_pub_[handle];
  }
  else
  {
    if (!global_pub_)
    {
      std::string topic_name("livox/lidar");
      queue_size = queue_size * 8;  // shared queue size is 256, for all lidars
      global_pub_ = CreatePublisher(transfer_format_, topic_name, queue_size);
    }
    return global_pub_;
  }
}

std::shared_ptr<rclcpp::PublisherBase> Lddc::GetCurrentImuPublisher(uint8_t handle)
{
  uint32_t queue_size = kMinEthPacketQueueSize;
  if (use_multi_topic_)
  {
    if (!private_imu_pub_[handle])
    {
      char name_str[48];
      memset(name_str, 0, sizeof(name_str));
      std::string ip_string = IpNumToString(lds_->lidars_[handle].handle);
      snprintf(
        name_str, sizeof(name_str), "livox/imu_%s", ReplacePeriodByUnderline(ip_string).c_str());
      std::string topic_name(name_str);
      queue_size = queue_size * 2;  // queue size is 64 for only one lidar
      private_imu_pub_[handle] = CreatePublisher(kLivoxImuMsg, topic_name, queue_size);
    }
    return private_imu_pub_[handle];
  }
  else
  {
    if (!global_imu_pub_)
    {
      std::string topic_name("livox/imu");
      queue_size = queue_size * 8;  // shared queue size is 256, for all lidars
      global_imu_pub_ = CreatePublisher(kLivoxImuMsg, topic_name, queue_size);
    }
    return global_imu_pub_;
  }
}

}  // namespace livox_ros
