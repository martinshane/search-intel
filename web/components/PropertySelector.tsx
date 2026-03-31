import React, { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, AlertCircle, CheckCircle2 } from 'lucide-react';

interface Property {
  id: string;
  name: string;
  url?: string;
}

interface PropertySelectorProps {
  onPropertiesSelected: (gscProperty: string, ga4Property: string) => void;
  disabled?: boolean;
}

export default function PropertySelector({ onPropertiesSelected, disabled = false }: PropertySelectorProps) {
  const [gscProperties, setGscProperties] = useState<Property[]>([]);
  const [ga4Properties, setGa4Properties] = useState<Property[]>([]);
  const [selectedGscProperty, setSelectedGscProperty] = useState<string>('');
  const [selectedGa4Property, setSelectedGa4Property] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);

  useEffect(() => {
    fetchProperties();
  }, []);

  useEffect(() => {
    // Validate selection whenever properties change
    if (selectedGscProperty && selectedGa4Property) {
      validateProperties();
    } else {
      setValidationError(null);
    }
  }, [selectedGscProperty, selectedGa4Property]);

  const fetchProperties = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/properties', {
        method: 'GET',
        credentials: 'include',
      });

      if (!response.ok) {
        if (response.status === 401) {
          throw new Error('Authentication required. Please connect your accounts.');
        }
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to fetch properties');
      }

      const data = await response.json();

      if (!data.gsc_properties || !data.ga4_properties) {
        throw new Error('Invalid response format from server');
      }

      setGscProperties(data.gsc_properties);
      setGa4Properties(data.ga4_properties);

      // Auto-select if only one property available for each
      if (data.gsc_properties.length === 1) {
        setSelectedGscProperty(data.gsc_properties[0].id);
      }
      if (data.ga4_properties.length === 1) {
        setSelectedGa4Property(data.ga4_properties[0].id);
      }
    } catch (err) {
      console.error('Error fetching properties:', err);
      setError(err instanceof Error ? err.message : 'An unexpected error occurred');
    } finally {
      setLoading(false);
    }
  };

  const validateProperties = async () => {
    if (!selectedGscProperty || !selectedGa4Property) {
      setValidationError(null);
      return;
    }

    try {
      const response = await fetch('/api/properties/validate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          gsc_property: selectedGscProperty,
          ga4_property: selectedGa4Property,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Validation failed');
      }

      const data = await response.json();

      if (!data.valid) {
        setValidationError(data.message || 'Properties do not match. Please ensure they are for the same website.');
      } else {
        setValidationError(null);
      }
    } catch (err) {
      console.error('Error validating properties:', err);
      setValidationError(err instanceof Error ? err.message : 'Validation error occurred');
    }
  };

  const handleConfirmSelection = () => {
    if (selectedGscProperty && selectedGa4Property && !validationError) {
      onPropertiesSelected(selectedGscProperty, selectedGa4Property);
    }
  };

  const getGscPropertyDisplay = (property: Property): string => {
    // Clean up the property display name
    if (property.url) {
      return property.url.replace(/^(sc-domain:|https?:\/\/)/, '').replace(/\/$/, '');
    }
    return property.name;
  };

  const getGa4PropertyDisplay = (property: Property): string => {
    return property.name;
  };

  const isSelectionValid = selectedGscProperty && selectedGa4Property && !validationError;

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Select Properties</CardTitle>
          <CardDescription>Loading your Google Search Console and GA4 properties...</CardDescription>
        </CardHeader>
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Select Properties</CardTitle>
          <CardDescription>Unable to load properties</CardDescription>
        </CardHeader>
        <CardContent>
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
          <Button onClick={fetchProperties} className="mt-4" variant="outline">
            Try Again
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (gscProperties.length === 0 || ga4Properties.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Select Properties</CardTitle>
          <CardDescription>No properties found</CardDescription>
        </CardHeader>
        <CardContent>
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              {gscProperties.length === 0 && ga4Properties.length === 0
                ? 'No Google Search Console or GA4 properties found. Please ensure you have access to at least one property in each service.'
                : gscProperties.length === 0
                ? 'No Google Search Console properties found. Please ensure you have verified properties in Search Console.'
                : 'No GA4 properties found. Please ensure you have GA4 properties set up with appropriate access.'}
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Select Properties</CardTitle>
        <CardDescription>
          Choose the Google Search Console property and GA4 property for the website you want to analyze.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-2">
          <Label htmlFor="gsc-property">Google Search Console Property</Label>
          <Select
            value={selectedGscProperty}
            onValueChange={setSelectedGscProperty}
            disabled={disabled || gscProperties.length === 0}
          >
            <SelectTrigger id="gsc-property">
              <SelectValue placeholder="Select a Search Console property" />
            </SelectTrigger>
            <SelectContent>
              {gscProperties.map((property) => (
                <SelectItem key={property.id} value={property.id}>
                  {getGscPropertyDisplay(property)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="ga4-property">Google Analytics 4 Property</Label>
          <Select
            value={selectedGa4Property}
            onValueChange={setSelectedGa4Property}
            disabled={disabled || ga4Properties.length === 0}
          >
            <SelectTrigger id="ga4-property">
              <SelectValue placeholder="Select a GA4 property" />
            </SelectTrigger>
            <SelectContent>
              {ga4Properties.map((property) => (
                <SelectItem key={property.id} value={property.id}>
                  {getGa4PropertyDisplay(property)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {validationError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{validationError}</AlertDescription>
          </Alert>
        )}

        {selectedGscProperty && selectedGa4Property && !validationError && (
          <Alert>
            <CheckCircle2 className="h-4 w-4" />
            <AlertDescription>
              Properties validated successfully. These appear to be for the same website.
            </AlertDescription>
          </Alert>
        )}

        <div className="flex justify-end pt-4">
          <Button
            onClick={handleConfirmSelection}
            disabled={!isSelectionValid || disabled}
            size="lg"
          >
            Continue to Report Generation
          </Button>
        </div>

        <div className="text-sm text-muted-foreground pt-4 border-t">
          <p className="font-medium mb-2">Need help?</p>
          <ul className="space-y-1 list-disc list-inside">
            <li>Make sure you have owner or full user access to both properties</li>
            <li>Properties should be for the same website domain</li>
            <li>GA4 property should be actively collecting data</li>
            <li>Search Console property should have at least 3 months of data</li>
          </ul>
        </div>
      </CardContent>
    </Card>
  );
}
