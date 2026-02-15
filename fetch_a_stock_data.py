#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股历史数据获取与分析脚本
使用AKShare获取近一年A股交易数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
import time

# 设置时间范围（近一年）
END_DATE = datetime.now().strftime('%Y%m%d')
START_DATE = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

print(f"数据获取时间范围: {START_DATE} 至 {END_DATE}")

# 创建输出目录
OUTPUT_DIR = "/root/.openclaw/workspace/a_stock_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_stock_list():
    """获取A股股票列表"""
    print("正在获取A股股票列表...")
    stock_df = ak.stock_zh_a_spot_em()
    print(f"获取到 {len(stock_df)} 只股票")
    return stock_df

def get_index_data():
    """获取主要指数数据"""
    print("\n正在获取主要指数数据...")
    
    indices = {
        '上证指数': '000001',
        '深证成指': '399001', 
        '创业板指': '399006',
        '科创50': '000688',
        '沪深300': '000300',
        '上证50': '000016'
    }
    
    index_data = {}
    for name, code in indices.items():
        try:
            if code.startswith('000'):
                df = ak.index_zh_a_hist(symbol=code, period="daily", 
                                       start_date=START_DATE, end_date=END_DATE)
            else:
                df = ak.index_zh_a_hist(symbol=code, period="daily",
                                       start_date=START_DATE, end_date=END_DATE)
            index_data[name] = df
            print(f"  ✓ {name}: {len(df)} 条数据")
            time.sleep(0.5)  # 避免请求过快
        except Exception as e:
            print(f"  ✗ {name}: {e}")
    
    return index_data

def get_stock_history(symbol, name=""):
    """获取单只股票历史数据"""
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                               start_date=START_DATE, end_date=END_DATE,
                               adjust="qfq")  # 前复权
        df['股票代码'] = symbol
        df['股票名称'] = name
        return df
    except Exception as e:
        print(f"  获取 {symbol} 失败: {e}")
        return None

def get_sample_stocks(stock_list, n=100):
    """获取样本股票数据（按市值排序取前N只）"""
    print(f"\n正在获取前{n}只市值最大股票的历史数据...")
    
    # 按市值排序
    stock_list_sorted = stock_list.sort_values('总市值', ascending=False)
    top_stocks = stock_list_sorted.head(n)
    
    all_data = []
    for idx, row in top_stocks.iterrows():
        symbol = row['代码']
        name = row['名称']
        df = get_stock_history(symbol, name)
        if df is not None:
            all_data.append(df)
            print(f"  ✓ {symbol} {name}: {len(df)} 条数据")
        time.sleep(0.3)  # 控制请求频率
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None

def analyze_data(index_data, stock_data, stock_list):
    """分析数据并生成报告"""
    print("\n" + "="*60)
    print("A股市场年度分析报告")
    print("="*60)
    
    report = []
    report.append(f"分析时间范围: {START_DATE} 至 {END_DATE}")
    report.append(f"分析股票数量: {len(stock_data['股票代码'].unique()) if stock_data is not None else 0}")
    report.append("")
    
    # 1. 指数表现分析
    report.append("【一、主要指数表现】")
    report.append("-" * 40)
    
    for name, df in index_data.items():
        if df is not None and len(df) > 0:
            start_price = df['收盘'].iloc[0]
            end_price = df['收盘'].iloc[-1]
            change_pct = (end_price - start_price) / start_price * 100
            max_price = df['最高'].max()
            min_price = df['最低'].min()
            volatility = ((max_price - min_price) / start_price) * 100
            
            report.append(f"\n{name} ({df['代码'].iloc[0] if '代码' in df.columns else 'N/A'}):")
            report.append(f"  期初收盘: {start_price:.2f}")
            report.append(f"  期末收盘: {end_price:.2f}")
            report.append(f"  涨跌幅: {change_pct:+.2f}%")
            report.append(f"  年内最高: {max_price:.2f}")
            report.append(f"  年内最低: {min_price:.2f}")
            report.append(f"  波动幅度: {volatility:.2f}%")
    
    # 2. 个股表现分析
    if stock_data is not None and len(stock_data) > 0:
        report.append("\n\n【二、个股表现统计】")
        report.append("-" * 40)
        
        # 按股票分组计算涨跌幅
        stock_returns = []
        for symbol in stock_data['股票代码'].unique():
            stock_df = stock_data[stock_data['股票代码'] == symbol]
            if len(stock_df) > 0:
                name = stock_df['股票名称'].iloc[0]
                start_price = stock_df['收盘'].iloc[0]
                end_price = stock_df['收盘'].iloc[-1]
                change_pct = (end_price - start_price) / start_price * 100
                max_price = stock_df['最高'].max()
                min_price = stock_df['最低'].min()
                volatility = ((max_price - min_price) / start_price) * 100
                avg_volume = stock_df['成交量'].mean()
                
                stock_returns.append({
                    '代码': symbol,
                    '名称': name,
                    '涨跌幅': change_pct,
                    '波动率': volatility,
                    '平均成交量': avg_volume
                })
        
        returns_df = pd.DataFrame(stock_returns)
        
        # 涨幅榜TOP10
        report.append("\n涨幅榜 TOP10:")
        top_gainers = returns_df.nlargest(10, '涨跌幅')
        for idx, row in top_gainers.iterrows():
            report.append(f"  {row['代码']} {row['名称']}: {row['涨跌幅']:+.2f}%")
        
        # 跌幅榜TOP10
        report.append("\n跌幅榜 TOP10:")
        top_losers = returns_df.nsmallest(10, '涨跌幅')
        for idx, row in top_losers.iterrows():
            report.append(f"  {row['代码']} {row['名称']}: {row['涨跌幅']:+.2f}%")
        
        # 整体统计
        report.append("\n整体统计:")
        report.append(f"  平均涨跌幅: {returns_df['涨跌幅'].mean():.2f}%")
        report.append(f"  涨跌幅中位数: {returns_df['涨跌幅'].median():.2f}%")
        report.append(f"  上涨股票数: {(returns_df['涨跌幅'] > 0).sum()}")
        report.append(f"  下跌股票数: {(returns_df['涨跌幅'] < 0).sum()}")
        report.append(f"  平盘股票数: {(returns_df['涨跌幅'] == 0).sum()}")
        
        # 波动率分析
        report.append("\n波动率分析:")
        report.append(f"  平均波动率: {returns_df['波动率'].mean():.2f}%")
        report.append(f"  最大波动率: {returns_df['波动率'].max():.2f}%")
        report.append(f"  最小波动率: {returns_df['波动率'].min():.2f}%")
    
    # 3. 市场概况
    report.append("\n\n【三、市场概况】")
    report.append("-" * 40)
    if stock_list is not None:
        total_market_cap = stock_list['总市值'].sum() / 1e8  # 转换为亿元
        report.append(f"A股总市值: {total_market_cap:,.0f} 亿元")
        report.append(f"上市公司数: {len(stock_list)} 家")
        
        # 成交额统计
        total_amount = stock_list['成交额'].sum() / 1e8
        report.append(f"总成交额: {total_amount:,.0f} 亿元")
    
    report_text = "\n".join(report)
    print(report_text)
    
    return report_text

def save_to_excel(index_data, stock_data, stock_list, report_text):
    """保存数据到Excel文件"""
    excel_path = os.path.join(OUTPUT_DIR, "A股年度数据汇总.xlsx")
    
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # 1. 分析报告
        report_df = pd.DataFrame({'分析报告': report_text.split('\n')})
        report_df.to_excel(writer, sheet_name='分析报告', index=False)
        
        # 2. 指数数据 - 每个指数一个sheet
        for name, df in index_data.items():
            if df is not None:
                sheet_name = name[:10]  # Excel sheet名长度限制
                df.to_excel(writer, sheet_name=f'指数_{sheet_name}', index=False)
        
        # 3. 个股数据
        if stock_data is not None:
            stock_data.to_excel(writer, sheet_name='个股历史数据', index=False)
        
        # 4. 股票列表
        if stock_list is not None:
            stock_list.to_excel(writer, sheet_name='股票列表', index=False)
    
    print(f"\n数据已保存至: {excel_path}")
    return excel_path

def main():
    print("="*60)
    print("A股历史数据获取与分析工具")
    print("="*60)
    
    # 1. 获取股票列表
    stock_list = get_stock_list()
    
    # 2. 获取指数数据
    index_data = get_index_data()
    
    # 3. 获取样本股票数据（前100只市值最大股票）
    stock_data = get_sample_stocks(stock_list, n=100)
    
    # 4. 分析数据
    report_text = analyze_data(index_data, stock_data, stock_list)
    
    # 5. 保存到Excel
    excel_path = save_to_excel(index_data, stock_data, stock_list, report_text)
    
    # 6. 保存报告文本
    report_path = os.path.join(OUTPUT_DIR, "分析报告.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"报告已保存至: {report_path}")
    
    print("\n" + "="*60)
    print("数据获取与分析完成！")
    print("="*60)

if __name__ == "__main__":
    main()
