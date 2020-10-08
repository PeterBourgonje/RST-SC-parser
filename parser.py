import os
import codecs
import lxml.etree
import sys
import csv
from collections import defaultdict
import argparse

xmlp = lxml.etree.XMLParser(strip_cdata=False, resolve_entities=False, encoding='utf-8')

def parse_corpus(rootfolder):

    signals = os.path.join(rootfolder, 'Analyses', 'All_Files')
    if not os.path.exists(signals):
        sys.stderr.write('ERROR: Could not find folder: %s\n' % signals)
    txts = os.path.join(rootfolder, 'Corpus', 'All_Files')
    if not os.path.exists(txts):
        sys.stderr.write('ERROR: Could not find folder: %s\n' % txts)

    nr_signals = defaultdict(lambda : defaultdict(int))
    relation2count = defaultdict(int)
    relation2fullcount = defaultdict(int)
    relation2signalcount = defaultdict(lambda : defaultdict(int))
    signal2count_i = defaultdict(int) # this counts per individual case (i.e. cumulative)
    signal2count_c = defaultdict(int) # this counts per combination
    signal2relationcount_i = defaultdict(lambda : defaultdict(int))
    signal2relationcount_c = defaultdict(lambda : defaultdict(int))

    debug = codecs.open('debug.txt', 'w')
    for f in os.listdir(txts):
        rel2signr = defaultdict(int)
        txt = os.path.join(txts, f)
        signal = os.path.join(signals, f, 'Signal.xml')
        if not f.startswith('.'):
            text = '_'.join(codecs.open(txt).readlines())
            tree = lxml.etree.parse(codecs.open(signal), parser=xmlp)
            relation2features = defaultdict(lambda : defaultdict(list))
            for segment in tree.getroot().findall('.//segment'):
                start = int(segment.get('start'))
                end = int(segment.get('end'))
                relation = text[start:end]
                features = segment.get('features')
                feat = features.split(';')[-1]
                relation2features[(start, end)]['relations'].append(relation)
                relation2features[(start, end)]['features'].append(feat)
                signal2count_i[feat] += 1
                signal2relationcount_i[feat][relation] += 1
                
            for relation in relation2features:
                assert len(set(relation2features[relation]['relations'])) == 1 # if not, this segment has multiple different relations, should not be the case.
                reltype = relation2features[relation]['relations'][0]
                relation2count[reltype] += 1
                relation2fullcount[reltype] += len(relation2features[relation]['relations'])
                if 'unsure' in relation2features[relation]['features']:
                    #assert len(relation2features[relation]['relations']) == 1
                    nr_signals[reltype][0] += 1
                else:
                    nr_signals[reltype][len(relation2features[relation]['relations'])] += 1
                relation2signalcount[reltype][frozenset(relation2features[relation]['features'])] += 1
                signal2count_c[frozenset(relation2features[relation]['features'])] += 1
                signal2relationcount_c[frozenset(relation2features[relation]['features'])][reltype] += 1
                rel2signr[len(relation2features[relation]['features'])] += 1
        debug.write('FILE: %s %s\n' % (txt, rel2signr))
                
    ### printing by relation ###
    by_relation_output = codecs.open('relation_keys.txt', 'w')

    for key in sorted(relation2count.keys()):
        by_relation_output.write('%s\t%i\n' % (key, relation2count[key]))
    t1 = 0
    t2 = 0
    for rel in relation2count:
        t1 += relation2count[rel]
        t2 += relation2fullcount[rel]
    by_relation_output.write('\n\nTotal relations: %i/%i\n\n' % (t1, t2)) # first is counting annotations for same span once, second is counting per annotation

    for key in sorted(nr_signals.keys()):
        by_relation_output.write('%s (%i)\n' % (key, relation2count[key]))
        for i in sorted(nr_signals[key].keys()):
            if i == 0:
                by_relation_output.write('\t0 signal(s) (unsure): %i\n' % (nr_signals[key][i]))
            else:
                by_relation_output.write('\t%i signal(s): %i\n' % (i, nr_signals[key][i]))
        by_relation_output.write('\n')
        for feat in relation2signalcount[key]:
            by_relation_output.write('\t%s: %i\n' % (feat, relation2signalcount[key][feat]))
            

    ### printing by signal ###
    by_signal_output = codecs.open('signal_keys.txt', 'w')

    by_signal_output.write('Signal counts isolated:\n\n')
    for key in sorted(signal2count_i.keys()):
        by_signal_output.write('\t%s: %i\n' % (key, signal2count_i[key]))
    by_signal_output.write('\n\nSignal counts in combination:\n\n')
    for key in sorted(signal2count_c.keys()): # sorting not actually needed/possible for frozensets, I think...
        by_signal_output.write('\t%s: %i\n' % (key, signal2count_c[key]))
    by_signal_output.write('\n\nSignal counts (isolated) split up by relations:\n\n')
    for signal in sorted(signal2relationcount_i.keys()):
        by_signal_output.write('\t%s (%i)\n' % (signal, signal2count_i[signal]))
        for rel in signal2relationcount_i[signal]:
            by_signal_output.write('\t\t%s: %i\n' % (rel, signal2relationcount_i[signal][rel]))
    by_signal_output.write('\n\nSignal counts (combined) split up by relations:\n\n')
    for signal in sorted(signal2relationcount_c.keys()):
        by_signal_output.write('\t%s: %i\n' % (signal, signal2count_c[signal]))
        for rel in signal2relationcount_c[signal]:
            by_signal_output.write('\t\t%s: %i\n' % (rel, signal2relationcount_c[signal][rel]))


            
if __name__ == '__main__':


    # TODO: parse cmd args
    parser = argparse.ArgumentParser(description='Extract some copus statistics from the RST Signaling Corpus...')
    parser.add_argument('--rootfolder', '-r', type=str, help='root folder of the RST-SC')
    args = parser.parse_args()

    rootfolder = args.rootfolder
    if not os.path.exists(rootfolder):
        sys.stderr.write('ERROR: Root folder %s not found\n' % args.rootfolder)
    
    parse_corpus(rootfolder)
