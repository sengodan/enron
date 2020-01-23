'''
Usuage : python summarize-enron.py <data-file-name> [number_of_top_senders]

This program will produce the following three output files in the current location using date file enron-event-history-all.csv
1. Output_1_Summary.csv
2. Output_2_Top_X_Senders_Monthly_Emails.png
3. Output_3_Top_X_Senders_Monthly_Contacts.png

'''

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os.path
from datetime import datetime

_data_columns = ['unix_time', 'message_id', 'sender', 'recipients', 'topic', 'mode']

def get_data(data_file):
    if not os.path.exists(data_file):
        print("Data File %s does not exist" %data_file)
        sys.exit()

    df_data = pd.read_csv(data_file, header=None, names = _data_columns )
    print( "Fetched %d rows from %s" %(df_data.shape[0], data_file))
    return df_data

def preprocess(df_data):
    df_data['date_time'] = pd.to_datetime(df_data['unix_time'], unit='ms' )  # Converting to datetime format
    df_data['year_month'] = df_data['date_time'].apply(lambda x: x.strftime('%Y-%m')) # Used for aggregating by months
    df_data['year'] = df_data['date_time'].dt.year # For aggregating into months # Used for aggregating by years
    return df_data

## Unstack pipe seperated recipients into individual rows
def unstack_recipients(df_data):
    df_recipients = (df_data.recipients.str.split("|", expand=True)
                                              .set_index(df_data.message_id)
                                              .stack()
                                              .reset_index(level=1, drop=True)
                                              .reset_index()
                                              .rename(columns={0:"recipient"}))

    print( "Unstacked Recipients from %d to %d rows.." %(df_data.shape[0], df_recipients.shape[0] ))

    return df_recipients

#### For processing output 1 to combined sender and recipient summary.
def get_summary_by_contact(df_data, df_recipients):
    df_sender_summary = df_data.groupby(['sender']).size().reset_index(name='sent')
    df_sender_summary.rename(columns={'sender':'person'}, inplace = True )

    df_recipients_summary = df_recipients.groupby(['recipient']).size().reset_index(name='received')
    df_recipients_summary.rename(columns={'recipient':'person'}, inplace = True )
    df_summary = pd.merge(df_recipients_summary, df_sender_summary, how='outer', on=['person'] )

    df_summary['sent' ].fillna(0, inplace=True)
    df_summary['received' ].fillna(0, inplace=True)
    df_summary['sent'] = df_summary['sent'].astype(int)
    df_summary['received'] = df_summary['received'].astype(int)
    df_summary = df_summary.sort_values(by=['sent', 'person'], ascending=[True, True])

    print( "%d Senders & %d Recipients are merged into %d Contacts..." %(df_sender_summary.shape[0], df_recipients_summary.shape[0], df_summary.shape[0] ))
    df_summary[[ 'person', 'sent', 'received' ]].head(5)
    return df_summary[[ 'person', 'sent', 'received' ]]

def save_summary_results(df_summary, output_file):
    df_summary.to_csv(output_file, index=False, header=True)
    print("Saved the summary into %s" %output_file )

def get_top_senders_list(df_summary, no_of_senders) :
    return df_summary.sort_values('sent', ascending = False ).head(no_of_senders)['person'].tolist()

def filter_top_senders(df_data, top_senders_list) :
    return df_data[df_data['sender'].isin(top_senders_list )]

## Used for handling if any missing months in the data
def get_months_template(df_top_senders) :
    start_date, end_date = df_top_senders['date_time'].min(), df_top_senders['date_time'].max()
    month_list = [i.strftime("%Y-%m") for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    return pd.DataFrame({'year_month':month_list})

def get_top_senders_summary( df_top_senders ) :
    return df_top_senders.groupby(['sender', 'year_month']).size().reset_index(name='counts')

def get_top_senders_monthly_emails_chart ( df_top_senders, top_senders_list, output_file ) :
    df_top_senders_summary = get_top_senders_summary( df_top_senders )
    df_months = get_months_template(df_top_senders)
    fig, ax = plt.subplots()
    labels = []
    for i in range(len(top_senders_list)):
        df_sender_data = df_top_senders_summary[ df_top_senders_summary.sender == top_senders_list[i] ]
        df_sender_updated = pd.merge(df_months, df_sender_data, how='left', on=['year_month'] )
        df_sender_updated['counts'].fillna(0, inplace=True)
        plt.plot(df_sender_updated.year_month, df_sender_updated.counts, label= top_senders_list[i])
        labels.append(top_senders_list[i] )

    plt.title('Top Senders Emails')
    plt.legend(labels, loc='upper left', ncol = 1 )
    plt.xticks(rotation=90)
    plt.xlabel('Month')
    plt.ylabel('# of Emails Sent by Month')

    every_nth = 3
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)

    fig.savefig(output_file, dpi=fig.dpi, bbox_inches='tight', pad_inches=0.5)
    print("Saved the top senders monthly emails chart into %s" %output_file )

def get_top_senders_monthly_contacts_chart ( df_top_senders, top_senders_list, df_recipients, output_file ) :
    df_top_senders_exploded = pd.merge(df_top_senders, df_recipients, how='left', on=['message_id'] )
    total_unique = df_top_senders_exploded.recipient.nunique()

    df_top_senders_exploded_summary = df_top_senders_exploded.groupby(['sender', 'year_month']).recipient.nunique().reset_index(name='counts')
    df_top_senders_exploded_summary['email_pecentage'] = df_top_senders_exploded_summary.counts/ total_unique * 100
    df_months = get_months_template(df_top_senders)

    fig, ax = plt.subplots()
    labels = []
    for i in range(len(top_senders_list)):
        df_sender_data = df_top_senders_exploded_summary [ df_top_senders_exploded_summary.sender == top_senders_list[i] ]
        df_sender_updated = pd.merge(df_months, df_sender_data, how='left', on=['year_month'] )
        df_sender_updated['email_pecentage'].fillna(0, inplace=True)
        plt.plot(df_sender_updated.year_month, df_sender_updated.email_pecentage, label= top_senders_list[i])
        labels.append(top_senders_list[i] )

    plt.gca().set_yticklabels(['{:.0f}%'.format(x) for x in plt.gca().get_yticks()])

    plt.title('Top Senders Percentage of Contacts')
    plt.legend(labels, loc='upper left', ncol = 1 )
    plt.xticks(rotation=90)
    plt.xlabel('Month')
    plt.ylabel('% of Unique Monthly Receipients')

    every_nth = 3
    for n, label in enumerate(ax.xaxis.get_ticklabels()):
        if n % every_nth != 0:
            label.set_visible(False)

    fig.savefig(output_file, dpi=fig.dpi, bbox_inches='tight', pad_inches=0.5)
    print("Saved the top senders monthly contact chart into %s" %output_file )


def run(data_file, number_of_top_senders ) :
    result_file_1 = 'Output_1_Summary.csv'
    result_file_2 = 'Output_2_Top_%s_Senders_Monthly_Emails.png' %number_of_top_senders
    result_file_3 = 'Output_3_Top_%s_Senders_Monthly_Contacts.png' %number_of_top_senders
    try :
        print("************** Start of Processing ************** " )
        start_time = datetime.now()
        df_data = get_data(data_file)
        df_data = preprocess(df_data )
        df_recipients = unstack_recipients(df_data)
        df_summary = get_summary_by_contact(df_data, df_recipients)
        print(df_summary.head(5))
        save_summary_results(df_summary, result_file_1 )

        top_senders_list = get_top_senders_list(df_summary, number_of_top_senders)
        df_top_senders = filter_top_senders(df_data, top_senders_list)
        get_top_senders_monthly_emails_chart ( df_top_senders, top_senders_list, result_file_2 )
        get_top_senders_monthly_contacts_chart ( df_top_senders, top_senders_list, df_recipients, result_file_3 )

        end_time = datetime.now()
        print("Duration of Execution: {}".format(end_time - start_time ))
        print("************** End of Processing ************** " )

    except Exception as e:
        print(e.message if hasattr(e, 'message') else e)
        raise


## number_of_top_senders is an optional command line parameter, default is 5
if __name__ == "__main__":
    if len(sys.argv) > 1 :
        number_of_top_senders = 5
        data_file = sys.argv[1]
        if len(sys.argv) > 2 :
            number_of_top_senders = int(sys.argv[2])

        run(data_file, number_of_top_senders)
    else :
        print( "Enron Data File Name missing in the command line..." )
        print( "Usuage : python summarize-enron.py <data-file-name> [number_of_top_senders]" )
        sys.exit()
