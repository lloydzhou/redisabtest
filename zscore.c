#include <stdlib.h>
#include <math.h>

void real_mean_std(double mean, double std, long long user, long long uv, double *real_mean, double *real_std) {
  *real_mean = mean;
  double real_s = (std * std) * (user - 1);
  for (int i = user; i < uv; i++) {
    double score = 0;
    long long count = i + 1;
    real_s = real_s + (count - 1) / count * (score - *real_mean) * (score - *real_mean);
    *real_mean = *real_mean + (score - *real_mean) / count;
  }
  *real_std = sqrt(real_s / (uv - 1));
}

double zscore(double mean1, double std1, long long n1, double mean2, double std2, long long n2) {
  return (mean1 - mean2) / sqrt(std1 * std1 / n1 + std2 * std2 / n2);
}

// https://gist.github.com/izaac/daf81344fe6061c82172
double getZPercent(double z) {
  if (z < -6.5) {
    return 0.0;
  }
  if(z > 6.5) {
    return 1.0;
  }

  double factK = 1, sum = 0, term = 1, loopStop = exp(-23); 
  long long k = 0;
  while(fabs(term) > loopStop)
  {
      term = 0.3989422804 * pow(-1 , k) * pow(z , k) / (2 * k + 1) / pow(2 , k) * pow(z, k + 1) / factK;
      sum += term;
      k++;
      factK *= k;
  }
  sum += 0.5;
  return sum;
}

