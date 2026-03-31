import { Suspense } from 'react';
import { notFound } from 'next/navigation';
import { createServerComponentClient } from '@supabase/auth-helpers-nextjs';
import { cookies } from 'next/headers';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Module1Traffic } from '@/components/reports/Module1Traffic';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Activity,
  FileText,
  Search,
  Brain,
  Target,
  TrendingUp,
  Link as LinkIcon,
  Users,
  Calendar,
  Award,
  Zap,
  CheckCircle2,
} from 'lucide-react';

interface ReportPageProps {
  params: {
    id: string;
  };
}

interface ReportMetadata {
  id: string;
  site_url: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error_message?: string;
  data?: {
    health?: any;
    pages?: any;
    serp?: any;
    content?: any;
    gameplan?: any;
    algorithm?: any;
    intent?: any;
    internal_links?: any;
    seasonality?: any;
    competitor?: any;
    risk?: any;
    executive?: any;
  };
}

async function getReport(reportId: string): Promise<ReportMetadata | null> {
  const supabase = createServerComponentClient({ cookies });

  const { data, error } = await supabase
    .from('reports')
    .select('*')
    .eq('id', reportId)
    .single();

  if (error || !data) {
    return null;
  }

  return data as ReportMetadata;
}

function ReportHeader({ report }: { report: ReportMetadata }) {
  const statusConfig = {
    pending: { label: 'Pending', color: 'bg-gray-500' },
    processing: { label: 'Processing', color: 'bg-blue-500' },
    completed: { label: 'Completed', color: 'bg-green-500' },
    failed: { label: 'Failed', color: 'bg-red-500' },
  };

  const config = statusConfig[report.status];
  const createdDate = new Date(report.created_at).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const completedDate = report.completed_at
    ? new Date(report.completed_at).toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
      })
    : null;

  return (
    <div className="mb-8">
      <div className="flex items-start justify-between mb-4">
        <div>
          <h1 className="text-4xl font-bold mb-2">Search Intelligence Report</h1>
          <p className="text-xl text-muted-foreground">{report.site_url}</p>
        </div>
        <Badge className={config.color}>{config.label}</Badge>
      </div>

      <div className="flex gap-6 text-sm text-muted-foreground">
        <div className="flex items-center gap-2">
          <Calendar className="h-4 w-4" />
          <span>Created: {createdDate}</span>
        </div>
        {completedDate && (
          <div className="flex items-center gap-2">
            <CheckCircle2 className="h-4 w-4" />
            <span>Completed: {completedDate}</span>
          </div>
        )}
      </div>

      {report.error_message && (
        <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
          <p className="text-red-800 font-medium">Error:</p>
          <p className="text-red-600 text-sm mt-1">{report.error_message}</p>
        </div>
      )}
    </div>
  );
}

function ModulePlaceholder({
  title,
  description,
  icon: Icon,
}: {
  title: string;
  description: string;
  icon: any;
}) {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <Icon className="h-6 w-6 text-primary" />
          </div>
          <div>
            <CardTitle>{title}</CardTitle>
            <CardDescription>{description}</CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <Skeleton className="h-64 w-full" />
          <div className="grid gap-4 md:grid-cols-3">
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function ReportContent({ report }: { report: ReportMetadata }) {
  if (report.status === 'pending' || report.status === 'processing') {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Report Generation in Progress</CardTitle>
          <CardDescription>
            Your report is being generated. This typically takes 2-5 minutes.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-center py-12">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
              <p className="text-muted-foreground">
                Analyzing your data and generating insights...
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (report.status === 'failed') {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Report Generation Failed</CardTitle>
          <CardDescription>
            There was an error generating your report.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">
            Please try generating a new report. If the problem persists, contact
            support.
          </p>
        </CardContent>
      </Card>
    );
  }

  const hasData = report.data && Object.keys(report.data).length > 0;

  if (!hasData) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>No Data Available</CardTitle>
          <CardDescription>
            The report completed but no analysis data is available.
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Tabs defaultValue="module1" className="space-y-6">
      <TabsList className="grid w-full grid-cols-6 lg:grid-cols-12 gap-2">
        <TabsTrigger value="module1" className="text-xs">
          <Activity className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Traffic</span>
        </TabsTrigger>
        <TabsTrigger value="module2" className="text-xs">
          <FileText className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Pages</span>
        </TabsTrigger>
        <TabsTrigger value="module3" className="text-xs">
          <Search className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">SERP</span>
        </TabsTrigger>
        <TabsTrigger value="module4" className="text-xs">
          <Brain className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Content</span>
        </TabsTrigger>
        <TabsTrigger value="module5" className="text-xs">
          <Target className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Plan</span>
        </TabsTrigger>
        <TabsTrigger value="module6" className="text-xs">
          <TrendingUp className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Updates</span>
        </TabsTrigger>
        <TabsTrigger value="module7" className="text-xs">
          <Search className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Intent</span>
        </TabsTrigger>
        <TabsTrigger value="module8" className="text-xs">
          <LinkIcon className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Links</span>
        </TabsTrigger>
        <TabsTrigger value="module9" className="text-xs">
          <Calendar className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Season</span>
        </TabsTrigger>
        <TabsTrigger value="module10" className="text-xs">
          <Users className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Compete</span>
        </TabsTrigger>
        <TabsTrigger value="module11" className="text-xs">
          <Zap className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Risk</span>
        </TabsTrigger>
        <TabsTrigger value="module12" className="text-xs">
          <Award className="h-3 w-3 mr-1" />
          <span className="hidden sm:inline">Summary</span>
        </TabsTrigger>
      </TabsList>

      <TabsContent value="module1" className="space-y-6">
        <Module1Traffic reportId={report.id} data={report.data?.health} />
      </TabsContent>

      <TabsContent value="module2" className="space-y-6">
        <ModulePlaceholder
          title="Module 2: Page-Level Triage"
          description="Identify high-priority pages that need attention based on performance trends and engagement metrics"
          icon={FileText}
        />
      </TabsContent>

      <TabsContent value="module3" className="space-y-6">
        <ModulePlaceholder
          title="Module 3: SERP Landscape Analysis"
          description="Understand SERP features, competitor positioning, and click share opportunities"
          icon={Search}
        />
      </TabsContent>

      <TabsContent value="module4" className="space-y-6">
        <ModulePlaceholder
          title="Module 4: Content Intelligence"
          description="Detect cannibalization, striking distance opportunities, and content quality issues"
          icon={Brain}
        />
      </TabsContent>

      <TabsContent value="module5" className="space-y-6">
        <ModulePlaceholder
          title="Module 5: The Gameplan"
          description="Prioritized action items based on impact and effort, synthesized from all modules"
          icon={Target}
        />
      </TabsContent>

      <TabsContent value="module6" className="space-y-6">
        <ModulePlaceholder
          title="Module 6: Algorithm Update Impact Analysis"
          description="Identify how algorithm updates have affected your site and vulnerability patterns"
          icon={TrendingUp}
        />
      </TabsContent>

      <TabsContent value="module7" className="space-y-6">
        <ModulePlaceholder
          title="Module 7: Query Intent Migration Tracking"
          description="Track how search intent evolves over time for your key queries"
          icon={Search}
        />
      </TabsContent>

      <TabsContent value="module8" className="space-y-6">
        <ModulePlaceholder
          title="Module 8: Internal Link Authority Flow"
          description="Analyze PageRank distribution and internal linking opportunities"
          icon={LinkIcon}
        />
      </TabsContent>

      <TabsContent value="module9" className="space-y-6">
        <ModulePlaceholder
          title="Module 9: Seasonality & Content Calendar Intelligence"
          description="Discover seasonal patterns and optimal content timing strategies"
          icon={Calendar}
        />
      </TabsContent>

      <TabsContent value="module10" className="space-y-6">
        <ModulePlaceholder
          title="Module 10: Competitor Intelligence Dashboard"
          description="Track competitor movements and identify strategic opportunities"
          icon={Users}
        />
      </TabsContent>

      <TabsContent value="module11" className="space-y-6">
        <ModulePlaceholder
          title="Module 11: Risk & Dependency Mapping"
          description="Identify concentration risks and dependencies in your traffic portfolio"
          icon={Zap}
        />
      </TabsContent>

      <TabsContent value="module12" className="space-y-6">
        <ModulePlaceholder
          title="Module 12: Executive Summary"
          description="High-level overview with key metrics, trends, and recommendations"
          icon={Award}
        />
      </TabsContent>
    </Tabs>
  );
}

export default async function ReportPage({ params }: ReportPageProps) {
  const report = await getReport(params.id);

  if (!report) {
    notFound();
  }

  return (
    <div className="container mx-auto py-8 px-4 max-w-7xl">
      <ReportHeader report={report} />
      <Separator className="my-8" />
      <Suspense
        fallback={
          <Card>
            <CardContent className="py-12">
              <div className="flex items-center justify-center">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
              </div>
            </CardContent>
          </Card>
        }
      >
        <ReportContent report={report} />
      </Suspense>
    </div>
  );
}

export async function generateMetadata({ params }: ReportPageProps) {
  const report = await getReport(params.id);

  if (!report) {
    return {
      title: 'Report Not Found',
    };
  }

  return {
    title: `Search Intelligence Report - ${report.site_url}`,
    description: `Comprehensive search performance analysis for ${report.site_url}`,
  };
}
