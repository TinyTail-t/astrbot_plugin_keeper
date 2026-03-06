<div align="center">

# 🤖💰 AstrBot Keeper Plugin

**全 LLM AI 驱动的智能记账助手**

[![AstrBot](https://img.shields.io/badge/AstrBot-Plugin-blue?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZmlsbD0iY3VycmVudENvbG9yIiBkPSJNMTIgMkM2LjQ4IDIgMiA2LjQ4IDIgMTJzNC40OCAxMCAxMCAxMCAxMC00LjQ4IDEwLTEwUzE3LjUyIDIgMTIgMnptLTIgMTVsLTUtNSAxLjQxLTEuNDFMMTAgMTQuMTdsNy41OS03LjU5TDE5IDhsLTkgOXoiLz48L3N2Zz4=)](https://github.com/TinyTail-t/astrbot_plugin_keeper)
[![Python](https://img.shields.io/badge/Python-3.8+-green?style=flat-square&logo=python)](https://python.org)
[![SQLite](https://img.shields.io/badge/SQLite-3-orange?style=flat-square&logo=sqlite)](https://sqlite.org)
[![License](https://img.shields.io/badge/License-AGPL--3.0-blue?style=flat-square)](LICENSE)

*用自然语言，轻松管理你的每一分钱*

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [使用示例](#-使用示例) • [路线图](#-路线图)

</div>

---

## ✨ 功能特性

### 🗣️ 自然语言交互
- **零学习成本**：像聊天一样记账，无需记忆复杂命令
- **智能理解**：LLM 自动解析时间、金额、分类、标签
- **上下文感知**：支持多轮对话，持续跟踪记账场景

### 📊 强大的数据管理
| 功能 | 描述 |
|:---|:---|
| 📝 **智能记录** | 增删改查，支持时间、金额、描述、分类、标签 |
| 🏷️ **多级分类** | 树形结构分类系统，支持无限层级子分类 |
| 🎨 **标签系统** | 彩色标签，灵活标记，支持 AND/OR 多标签筛选 |
| 📈 **统计分析** | 按分类/标签/月份自动聚合，收支一目了然 |

### 🔒 安全与隐私
- **白名单机制**：用户访问控制，数据安全隔离
- **本地存储**：SQLite 数据库，数据完全自主掌控
- **用户隔离**：多用户独立空间，数据互不干扰

---

## 🚀 快速开始

```

配置

在 AstrBot 配置文件中添加白名单：
```
```
首次使用

用户: 注册记账功能
AI: ✅ 已成功为您开通记账服务！现在您可以：
    • 直接说"今天午餐花了25块"来记账
    • 问"这个月花了多少钱"查看统计
    • 使用"设置分类"管理账目类别
```

---

💡 使用示例

自然语言记账

你说	AI 理解并执行	
"刚才买咖啡花了32元"	✅ 创建支出记录：餐饮-咖啡，¥32	
"昨天发工资8000"	✅ 创建收入记录：工资，¥8000	
"每周五定投基金500"	🔄 创建周期性计划（开发中）	
"把刚才那条改成35块"	✅ 更新记录金额	

智能查询与统计

```
: 看看上个月餐饮花了多少
AI: 📊 2026年1月餐饮支出统计：
    总计：¥2,456.00
    日均：¥84.69
    最高单笔：¥388（2月14日 情人节晚餐）
    主要消费：火锅(¥520)、外卖(¥680)、超市(¥456)

: 找出所有带"报销"标签的
AI: 🔍 找到 12 条记录，合计待报销 ¥3,240.00
    [...]
```

分类与标签管理

```
: 创建"数码产品"分类，下面加"手机配件"和"电脑硬件"
AI: ✅ 已创建分类结构：
    📁 数码产品
    ├── 📁 手机配件
    └── 📁 电脑硬件

用户: 给重要支出打红色标记
AI: ✅ 已创建标签"重要"，颜色 #ff4d4f
    您可以说"标记为重要"来使用
```

---

🗺️ 路线图

✅ 已实现
- 自然语言记账（增删改查）
- 多级分类系统
- 彩色标签管理
- 多维度统计分析
- 白名单权限控制

🚧 开发中
- 数据导入导出 (CSV/Excel/JSON)
- 智能预算提醒：超支预警、定期报告
- 周期记账：自动记录房租、定投等固定支出

🔮 计划支持
- 多数据库支持：MySQL / PostgreSQL / MongoDB
- 跨数据库迁移：一键转移历史数据
- 数据可视化：图表生成、趋势分析
- OCR 识别：拍照自动识别发票、小票
- 多币种支持：汇率自动转换
- 家庭共享：多用户账本共享与权限管理

---

🏗️ 技术架构
